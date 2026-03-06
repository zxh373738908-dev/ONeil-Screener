import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import akshare as ak
import time
import requests
import json
import re

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
SHEET_URL = "你的GOOGLE表格链接填在这里"  # ⚠️ 记得换成你的真实链接！

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔选股模块 (保持原版严格突破逻辑)
# ==========================================
def screen_us_stocks():
    print("\n========== 开始处理美股 ==========")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].tolist()
        ndx100 = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)[4]['Ticker'].tolist()
        tickers = list(set([t.replace('.', '-') for t in (sp500 + ndx100)]))
    except Exception as e:
        print("获取美股列表失败")
        return []

    final_stocks = []
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1y")
            if len(hist) < 200: continue
            
            close = hist['Close'].iloc[-1]
            volume = hist['Volume'].iloc[-1]
            if close < 15 or (close * volume) < 50000000: continue
            
            ma20 = hist['Close'].rolling(20).mean().iloc[-1]
            ma50 = hist['Close'].rolling(50).mean().iloc[-1]
            ma200 = hist['Close'].rolling(200).mean().iloc[-1]
            if not (close > ma20 and ma20 > ma50 and close > ma200): continue
            
            high_250 = hist['High'].rolling(250).max().iloc[-1]
            if close < (high_250 * 0.85): continue
            
            ret_90d = (close - hist['Close'].iloc[-63]) / hist['Close'].iloc[-63]
            if ret_90d < 0.25: continue
            
            final_stocks.append({
                "Ticker": ticker,
                "Price": round(close, 2),
                "90D_Return%": f"{round(ret_90d * 100, 2)}%",
                "Turnover(M)": round((close * volume) / 1000000, 2),
                "Struct": "MA20>MA50"
            })
        except:
            continue
    return final_stocks

# ==========================================
# 3. [A股] 新浪财经高匿分页爬虫 (绕过防火墙)
# ==========================================
def get_sina_market_snapshot():
    print("🚀 启动【新浪财经】高匿分页拉取引擎...")
    all_data = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'Referer': 'http://finance.sina.com.cn/'
    }
    
    for page in range(1, 80):
        url = f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=80&sort=symbol&asc=1&node=hs_a"
        try:
            res = requests.get(url, headers=headers, timeout=5)
            text = res.text
            if text == "[]" or text == "null" or not text:
                break
            text = re.sub(r'([{,])\s*([a-zA-Z0-9_]+)\s*:', r'\1"\2":', text)
            data = json.loads(text)
            all_data.extend(data)
        except:
            continue
            
    print(f"✅ 隐身拉取成功！共获取全市场 {len(all_data)} 只股票基础数据。")
    return pd.DataFrame(all_data)

# ==========================================
# 4. [A股] 欧奈尔核心筛选模块 (A股高波动回调低吸版)
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (兼容主升浪回调洗盘标的) ==========")
    try:
        spot_df = get_sina_market_snapshot()
        if spot_df.empty: return []
        
        # 数据清洗与单位转换 (市值转为元)
        spot_df['trade'] = pd.to_numeric(spot_df['trade'], errors='coerce')
        spot_df['mktcap'] = pd.to_numeric(spot_df['mktcap'], errors='coerce') * 10000
        spot_df['amount'] = pd.to_numeric(spot_df['amount'], errors='coerce')
        spot_df['turnoverratio'] = pd.to_numeric(spot_df['turnoverratio'], errors='coerce')

        # 【优化1】市值门槛降至 50亿 (防止次新股流通盘计算差异错杀)，成交额维持 2亿 (确认主力资金活跃度)
        cond1 = spot_df['trade'] >= 10
        cond2 = spot_df['mktcap'] >= 5_000_000_000   # 50亿
        cond3 = spot_df['amount'] >= 200_000_000     # 2亿
        cond4 = spot_df['turnoverratio'] >= 1.5
            
        filtered_df = spot_df[cond1 & cond2 & cond3 & cond4].copy()
        print(f"第一轮初筛完成：满足 50亿市值/2亿成交额 的候选股剩 {len(filtered_df)} 只。开始扫描 K 线...")

        final_a_stocks = []
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=400)).strftime("%Y%m%d")
        
        for index, row in filtered_df.iterrows():
            raw_code = row['code']
            pure_code = raw_code[-6:] # 截取后6位纯数字
            name = row['name']
            
            try:
                time.sleep(0.1) # 停顿防止被拉黑
                hist = ak.stock_zh_a_hist(symbol=pure_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                if len(hist) < 250: continue
                    
                close = hist['收盘'].iloc[-1]
                
                # 动量要求：60日涨幅 >= 20%
                close_60 = hist['收盘'].iloc[-61]
                ret_60 = (close - close_60) / close_60
                if ret_60 < 0.20: continue
                
                # 【优化2】均线多头测算：允许股价暂时跌破20日线洗盘，但必须站稳60日/120日线，且中期趋势(MA20>MA60)保持向上
                ma20 = hist['收盘'].rolling(20).mean().iloc[-1]
                ma60 = hist['收盘'].rolling(60).mean().iloc[-1]
                ma120 = hist['收盘'].rolling(120).mean().iloc[-1]
                if not (close > ma60 and close > ma120): continue # 守住生命线
                if not (ma20 > ma60): continue # 中期多头排列未被破坏
                    
                # 【优化3】突破历史新高测算：容忍250日最高价向下回撤 20% (A股杯柄形态常见深度)
                high_250 = hist['最高'].rolling(250).max().iloc[-1]
                if close < (high_250 * 0.80): continue 
                    
                # 【优化4】RSI 测算：底线降至 50，允许短期情绪降温，只要没进入空头趋势即可
                delta = hist['收盘'].diff()
                up = delta.clip(lower=0)
                down = -1 * delta.clip(upper=0)
                ema_up = up.ewm(com=13, adjust=False).mean()
                ema_down = down.ewm(com=13, adjust=False).mean()
                rs = ema_up / ema_down
                rsi = 100 - (100 / (1 + rs)).iloc[-1]
                if rsi < 50: continue
                    
                # ================= 满足条件，加入白名单 =================
                final_a_stocks.append({
                    "Ticker": pure_code,
                    "Name": name,
                    "Price": round(close, 2),
                    "60D_Return%": f"{round(ret_60 * 100, 2)}%",
                    "Turnover(亿)": round(row['amount'] / 100_000_000, 2),
                    "Turnover_Rate%": f"{row['turnoverratio']}%",
                    "RSI": round(rsi, 2),
                    "Trend": "Hold MA60",
                    "Fundamental": "Check ROE>15%" 
                })
                print(f"🎯 捕获主升浪标的: {pure_code} {name} (60日涨幅 {round(ret_60 * 100, 2)}%)")
                
            except Exception as e:
                continue
                
        return final_a_stocks
        
    except Exception as e:
        print(f"A 股扫描发生致命错误: {e}")
        return []

# ==========================================
# 5. 写入 Google Sheets
# ==========================================
def write_to_sheet(sheet_name, final_stocks, sort_col):
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            df['Sort_Num'] = df[sort_col].str.replace('%', '').astype(float)
            df = df.sort_values(by='Sort_Num', ascending=False).drop(columns=['Sort_Num'])
            
            data_to_write = [df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_cell(1, len(df.columns) + 2, "Last Updated:")
            sheet.update_cell(1, len(df.columns) + 3, now_time)
            print(f"🎉 成功将 {len(df)} 只最强龙头写入 {sheet_name}！")
        else:
            sheet.clear()
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("A1", f"[{now_time}] 当下大盘无个股满足条件 (50亿市值/2亿成交额/60日涨幅超20%)。")
            print(f"⚠️ {sheet_name}: 无符合条件的股票。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

# ==========================================
# 6. 主程序启动
# ==========================================
if __name__ == "__main__":
    us_results = screen_us_stocks()
    write_to_sheet("Screener", us_results, sort_col="90D_Return%")
    
    a_results = screen_a_shares()
    write_to_sheet("A-Share Screener", a_results, sort_col="60D_Return%")
