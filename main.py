import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import time
import requests
import json
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=913399386#gid=913399386"  # ⚠️ 记得换成你的真实链接！

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔选股模块 (黄金坑回调低吸版)
# ==========================================
def screen_us_stocks():
    print("\n========== 开始处理美股 (兼容机构护盘与黄金坑标的) ==========")
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
            ma150 = hist['Close'].rolling(150).mean().iloc[-1]
            ma200 = hist['Close'].rolling(200).mean().iloc[-1]
            
            if not (ma50 > ma200): continue
            if not (close > ma150 and close > ma200): continue
            
            high_250 = hist['High'].rolling(250).max().iloc[-1]
            if close < (high_250 * 0.80): continue
            
            ret_90d = (close - hist['Close'].iloc[-63]) / hist['Close'].iloc[-63]
            if ret_90d < 0.15: continue
            
            delta = hist['Close'].diff()
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            ema_up = up.ewm(com=13, adjust=False).mean()
            ema_down = down.ewm(com=13, adjust=False).mean()
            rs = ema_up / ema_down
            rsi = 100 - (100 / (1 + rs)).iloc[-1]
            if rsi < 45: continue
            
            struct_label = "Breakout (>MA20)" if close > ma20 else "Pullback (Near MA50)"
            
            final_stocks.append({
                "Ticker": ticker,
                "Price": round(close, 2),
                "90D_Return%": f"{round(ret_90d * 100, 2)}%",
                "Turnover(M)": round((close * volume) / 1000000, 2),
                "Struct": struct_label
            })
        except:
            continue
    return final_stocks

# ==========================================
# 3. [A股] 东方财富底层原生接口 (绝对防屏蔽)
# ==========================================
def get_eastmoney_spot():
    print("🚀 启动【东方财富底层原生 API】拉取全市场数据...")
    all_data = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 抓取全市场 (f2=最新价, f6=成交额, f8=换手率, f12=代码, f14=名称, f20=总市值)
    for page in range(1, 60):
        url = f"http://82.push2.eastmoney.com/api/qt/clist/get?pn={page}&pz=100&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f2,f6,f8,f12,f14,f20"
        try:
            res = requests.get(url, headers=headers, timeout=5).json()
            if not res.get('data') or not res['data'].get('diff'):
                break
            all_data.extend(res['data']['diff'])
        except:
            continue
            
    df = pd.DataFrame(all_data)
    print(f"✅ 获取 A 股 {len(df)} 只股票基础数据成功！")
    return df

# ==========================================
# 4. [A股] 欧奈尔核心筛选模块 (100% 防漏极速版)
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (50亿/2亿/原生K线计算版) ==========")
    try:
        spot_df = get_eastmoney_spot()
        if spot_df.empty: return []
        
        # 将空数据 "-" 替换为 NaN，并转为数字
        spot_df = spot_df.replace("-", pd.NA)
        spot_df['f2'] = pd.to_numeric(spot_df['f2'], errors='coerce')   # 价格
        spot_df['f20'] = pd.to_numeric(spot_df['f20'], errors='coerce') # 总市值
        spot_df['f6'] = pd.to_numeric(spot_df['f6'], errors='coerce')   # 成交额
        spot_df['f8'] = pd.to_numeric(spot_df['f8'], errors='coerce')   # 换手率

        # 【第一道漏斗】：只过滤基础门槛 (抛弃容易缺失的涨跌幅字段，100%防漏)
        cond1 = spot_df['f2'] >= 10
        cond2 = spot_df['f20'] >= 5_000_000_000   # 50亿市值
        cond3 = spot_df['f6'] >= 200_000_000      # 2亿成交额
        cond4 = spot_df['f8'] >= 1.5              # 1.5%换手
            
        filtered_df = spot_df[cond1 & cond2 & cond3 & cond4].copy()
        print(f"🎯 漏斗1 (流动性过滤): 满足 50亿/2亿 的核心标的剩余 {len(filtered_df)} 只。开始深入 K 线扫描...")

        final_a_stocks = []
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        # 记录淘汰原因，方便复盘
        fail_reasons = {"动量不足20%": 0, "破位MA60/120": 0, "回撤超20%": 0, "RSI弱势": 0}
        
        for index, row in filtered_df.iterrows():
            code = str(row['f12']).zfill(6)
            name = row['f14']
            secid = f"1.{code}" if code.startswith('6') else f"0.{code}"
            
            try:
                # 抓取 300 天 K 线
                k_url = f"http://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&end=20500101&lmt=300"
                res = requests.get(k_url, headers=headers, timeout=5).json()
                klines = res['data']['klines']
                
                closes = [float(k.split(',')[2]) for k in klines]
                highs = [float(k.split(',')[3]) for k in klines]
                
                if len(closes) < 250: continue
                
                close_series = pd.Series(closes)
                high_series = pd.Series(highs)
                close = close_series.iloc[-1]
                
                # 【漏斗2】：手动精准计算 60 日涨跌幅 >= 20%
                close_60 = close_series.iloc[-61]
                ret_60 = (close - close_60) / close_60
                if ret_60 < 0.20:
                    fail_reasons["动量不足20%"] += 1
                    continue
                
                # 【漏斗3】：均线多头 (守住 MA60 和 MA120)
                ma20 = close_series.rolling(20).mean().iloc[-1]
                ma60 = close_series.rolling(60).mean().iloc[-1]
                ma120 = close_series.rolling(120).mean().iloc[-1]
                if not (close > ma60 and close > ma120) or not (ma20 > ma60):
                    fail_reasons["破位MA60/120"] += 1
                    continue 
                    
                # 【漏斗4】：逼近历史新高 (容忍回撤20%)
                high_250 = high_series.rolling(250).max().iloc[-1]
                if close < (high_250 * 0.80):
                    fail_reasons["回撤超20%"] += 1
                    continue 
                    
                # 【漏斗5】：RSI > 50
                delta = close_series.diff()
                up = delta.clip(lower=0)
                down = -1 * delta.clip(upper=0)
                ema_up = up.ewm(com=13, adjust=False).mean()
                ema_down = down.ewm(com=13, adjust=False).mean()
                rs = ema_up / ema_down
                rsi = 100 - (100 / (1 + rs)).iloc[-1]
                if rsi < 50:
                    fail_reasons["RSI弱势"] += 1
                    continue
                    
                # ================= 白名单 =================
                final_a_stocks.append({
                    "Ticker": code,
                    "Name": name,
                    "Price": round(close, 2),
                    "60D_Return%": f"{round(ret_60 * 100, 2)}%",
                    "Turnover(亿)": round(row['f6'] / 100_000_000, 2),
                    "Turnover_Rate%": f"{row['f8']}%",
                    "RSI": round(rsi, 2),
                    "Trend": "Hold MA60",
                    "Fundamental": "Check ROE>15%" 
                })
                print(f"✅ 捕获主升浪标的: {code} {name} (60日涨幅 {round(ret_60 * 100, 2)}%)")
                
                # 停顿极短时间，接口超快不怕封
                time.sleep(0.02)
                
            except Exception as e:
                continue
                
        # 打印淘汰统计报告
        print(f"\n📊 淘汰统计报告:")
        for reason, count in fail_reasons.items():
            print(f" - 因【{reason}】被淘汰: {count} 只")
            
        return final_a_stocks
        
    except Exception as e:
        print(f"❌ A 股扫描发生致命错误: {e}")
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
