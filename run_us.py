import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import concurrent.futures
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# 1. Google Sheets 连接
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"  
scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔选股核心 (全参数补全版)
# ==========================================
def fetch_us_hist(ticker):
    stock = yf.Ticker(ticker)
    return stock.history(period="1y")

def screen_us_stocks():
    print("\n========== 开始处理美股 [全参数补全版] ==========")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        sp_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        sp500 = next(df['Symbol'].tolist() for df in sp_tables if 'Symbol' in df.columns)
        
        ndx_tables = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)
        ndx100 = next(df['Ticker'].tolist() for df in ndx_tables if 'Ticker' in df.columns)
        
        tickers = list(set([str(t).replace('.', '-') for t in (sp500 + ndx100)]))
        print(f"✅ 成功获取美股名单！共合并去重 {len(tickers)} 只核心股票。")
    except Exception as e:
        print(f"❌ 获取美股列表失败: {e}")
        return[]

    final_stocks =[]
    consecutive_fails = 0  
    
    for ticker in tickers:
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(fetch_us_hist, ticker)
                hist = future.result(timeout=8)
            
            if hist is None or len(hist) < 200: continue
            consecutive_fails = 0 
            
            close = hist['Close'].iloc[-1]
            volume = hist['Volume'].iloc[-1]
            
            # 【新增】欧奈尔突破放量指标：计算今日量比 (Vol Ratio)
            avg_vol_50 = hist['Volume'].tail(50).mean()
            vol_ratio = volume / avg_vol_50 if avg_vol_50 > 0 else 0
            
            if close < 15 or (close * volume) < 50000000: continue
            
            ma20 = hist['Close'].rolling(20).mean().iloc[-1]
            ma50 = hist['Close'].rolling(50).mean().iloc[-1]
            ma150 = hist['Close'].rolling(150).mean().iloc[-1]
            ma200 = hist['Close'].rolling(200).mean().iloc[-1]
            
            if not (ma50 > ma200) or not (close > ma150 and close > ma200): continue
            
            high_250 = hist['High'].rolling(250).max().iloc[-1]
            if close < (high_250 * 0.80): continue
            
            # 【新增】欧奈尔新高指标：计算当前距离52周最高点的距离
            dist_high = (close - high_250) / high_250
            
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
                "RSI": round(rsi, 2),
                "Vol_Ratio": round(vol_ratio, 2),
                "Dist_High%": f"{round(dist_high * 100, 2)}%",
                "Turnover(M)": round((close * volume) / 1000000, 2),
                "Struct": struct_label
            })
            print(f"✅ 捕获美股标的: {ticker}")
            
        except Exception as e:
            consecutive_fails += 1
            if consecutive_fails > 20:
                print("\n⚠️ 连续 20 只美股获取失败！雅虎财经封锁 IP，触发熔断！")
                break 
            continue
            
    return final_stocks

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
            sheet.update_acell(f"I1", "Last Updated:")
            sheet.update_acell(f"J1", now_time)
            print(f"🎉 成功将 {len(df)} 只最强龙头写入 {sheet_name}！")
        else:
            sheet.clear()
            msg = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 当下无符合条件的股票，或遇到极端行情/熔断。"
            sheet.update_acell("A1", msg)
            print(f"⚠️ {sheet_name}: 已输出空仓警告。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

if __name__ == "__main__":
    us_results = screen_us_stocks()
    write_to_sheet("Screener", us_results, sort_col="90D_Return%")
