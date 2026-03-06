import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import akshare as ak
import time

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
SHEET_URL = "你的GOOGLE表格链接填在这里"  # ⚠️ 记得换成你的真实链接！

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔选股模块 (保留你原有的美股逻辑)
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
# 3. [A股] 机构主升浪终极过滤模块 (V2.0 极速优化版)
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (极速优化版) ==========")
    try:
        # 1. 获取全市场 A 股实时行情
        print("正在获取东方财富全市场实时数据...")
        spot_df = ak.stock_zh_a_spot_em()
        
        # 强制转换数据类型
        spot_df['最新价'] = pd.to_numeric(spot_df['最新价'], errors='coerce')
        spot_df['总市值'] = pd.to_numeric(spot_df['总市值'], errors='coerce')
        spot_df['成交额'] = pd.to_numeric(spot_df['成交额'], errors='coerce')
        spot_df['换手率'] = pd.to_numeric(spot_df['换手率'], errors='coerce')
        spot_df['60日涨跌幅'] = pd.to_numeric(spot_df['60日涨跌幅'], errors='coerce') # 直接获取60日涨幅
        
        # 2. 核心前置过滤 (速度提升 10 倍的关键)
        # 总市值 ≥ 150亿(15,000,000,000), 股价 ≥ 10, 日成交额 ≥ 5亿, 换手率 ≥ 1.5%, 60日涨幅 ≥ 30%
        cond1 = spot_df['最新价'] >= 10
        cond2 = spot_df['总市值'] >= 15_000_000_000  # 之前这里多写了个0，已修复为 150亿
        cond3 = spot_df['成交额'] >= 500_000_000
        cond4 = spot_df['换手率'] >= 1.5
        cond5 = spot_df['60日涨跌幅'] >= 30  # 动量前置：连 30% 都没有的直接踢掉
        
        filtered_df = spot_df[cond1 & cond2 & cond3 & cond4 & cond5].copy()
        print(f"第一轮漏斗过滤完成：全市场 5000 只股票中，满足【150亿市值+动量主升浪】的候选股共有 {len(filtered_df)} 只。")
        
        final_a_stocks = []
        # 准备时间范围 (抓取过去400天数据，确保有足够的 250 个交易日)
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=400)).strftime("%Y%m%d")
        
        # 3. 深入测算这几十只候选股的技术形态
        for index, row in filtered_df.iterrows():
            code = row['代码']
            name = row['名称']
            try:
                # 获取日K线历史 (前复权)
                hist = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                if len(hist) < 250:
                    continue
                    
                close = hist['收盘'].iloc[-1]
                
                # A. 均线多头测算
                ma20 = hist['收盘'].rolling(20).mean().iloc[-1]
                ma60 = hist['收盘'].rolling(60).mean().iloc[-1]
                ma120 = hist['收盘'].rolling(120).mean().iloc[-1]
                
                # 收盘价 > MA20 > MA60 > MA120 (严格多头排列)
                if not (close > ma20 and ma20 > ma60 and ma60 > ma120): continue
                    
                # B. 突破位置测算 (当前价 >= 250日最高价的 85%)
                high_250 = hist['最高'].rolling(250).max().iloc[-1]
                if close < (high_250 * 0.85): continue
                    
                # C. RSI (14) >= 55 测算
                delta = hist['收盘'].diff()
                up = delta.clip(lower=0)
                down = -1 * delta.clip(upper=0)
                ema_up = up.ewm(com=13, adjust=False).mean()
                ema_down = down.ewm(com=13, adjust=False).mean()
                rs = ema_up / ema_down
                rsi = 100 - (100 / (1 + rs)).iloc[-1]
                if rsi < 55: continue
                    
                # ================= 满足所有终极条件，加入白名单 =================
                final_a_stocks.append({
                    "Ticker": code,
                    "Name": name,
                    "Price": round(close, 2),
                    "60D_Return%": f"{round(row['60日涨跌幅'], 2)}%",
                    "Turnover(亿)": round(row['成交额'] / 100_000_000, 2),
                    "Turnover_Rate%": f"{row['换手率']}%",
                    "RSI": round(rsi, 2),
                    "Trend": "MA20>60>120",
                    "Score": "Check ROE>15%" # 提醒看基本面
                })
                print(f"✅ 捕获 A 股欧奈尔硬核资产: {code} {name}")
                
                # 停顿0.1秒，防止被服务器拉黑
                time.sleep(0.1)
                
            except Exception as e:
                print(f"读取 {name} K线时出错，跳过...")
                continue
                
        return final_a_stocks
        
    except Exception as e:
        print(f"A 股扫描发生致命错误: {e}")
        return []

# ==========================================
# 4. 写入 Google Sheets
# ==========================================
def write_to_sheet(sheet_name, final_stocks, sort_col):
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            # 按涨幅降序排序，把最暴力的票放前面
            df['Sort_Num'] = df[sort_col].str.replace('%', '').astype(float)
            df = df.sort_values(by='Sort_Num', ascending=False).drop(columns=['Sort_Num'])
            
            data_to_write = [df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_cell(1, len(df.columns) + 2, "Last Updated:")
            sheet.update_cell(1, len(df.columns) + 3, now_time)
            print(f"🎉 成功将 {len(df)} 只欧奈尔主升浪股票写入 {sheet_name}！")
        else:
            sheet.clear()
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("A1", f"[{now_time}] 当下大盘较弱，全市场无一只股票同时满足：150亿市值、5亿成交额、60日涨幅超30%、且均线多头排列。空仓等待！")
            print(f"⚠️ {sheet_name}: 筛选严格，今天无符合条件的股票。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

# ==========================================
# 5. 主程序启动
# ==========================================
if __name__ == "__main__":
    # 执行美股
    us_results = screen_us_stocks()
    write_to_sheet("Screener", us_results, sort_col="90D_Return%")
    
    # 执行A股
    a_results = screen_a_shares()
    write_to_sheet("A-Share Screener", a_results, sort_col="60D_Return%")
