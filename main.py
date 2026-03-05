import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
# 填入你的 Google 表格的完整链接或 ID
SHEET_URL = "你的GOOGLE表格链接填在这里" 

# 连接 Google Sheets
scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)
sheet = client.open_by_url(SHEET_URL).worksheet("Screener")

# ==========================================
# 2. 获取股票池 (为了速度，这里抓取标普500+纳斯达克100作为示范池)
# 你也可以换成全量美股 Ticker 列表
# ==========================================
def get_tickers():
    # 抓取标普500
    sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]['Symbol'].tolist()
    # 抓取纳斯达克100
    ndx100 = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[4]['Ticker'].tolist()
    # 合并去重
    tickers = list(set(sp500 + ndx100))
    return[t.replace('.', '-') for t in tickers] # yfinance的格式修复

tickers = get_tickers()
print(f"开始筛选股票池，共 {len(tickers)} 只股票...")

# ==========================================
# 3. 欧奈尔选股逻辑执行
# ==========================================
final_stocks =[]

for ticker in tickers:
    try:
        stock = yf.Ticker(ticker)
        # 获取过去1年的日线数据
        hist = stock.history(period="1y")
        
        if len(hist) < 250: # 上市不满一年跳过
            continue
            
        # --- 核心数据计算 ---
        close = hist['Close'].iloc[-1]
        volume = hist['Volume'].iloc[-1]
        
        # 1. 市值与价格 (yfinance获取市值较慢，这里用粗略近似或通过价格/成交量先行过滤)
        if close < 15:
            continue
            
        # 2. 成交额 >= 5000万 (50,000,000)
        turnover = close * volume
        if turnover < 50000000:
            continue
            
        # 3. 均线系统 (MA20, MA50, MA200)
        ma20 = hist['Close'].rolling(window=20).mean().iloc[-1]
        ma50 = hist['Close'].rolling(window=50).mean().iloc[-1]
        ma200 = hist['Close'].rolling(window=200).mean().iloc[-1]
        
        # 条件：价格 > MA20 > MA50 且 价格 > MA200 (超级牛股过滤器)
        if not (close > ma20 and ma20 > ma50 and close > ma200):
            continue
            
        # 4. 250日最高价的 85%
        high_250 = hist['High'].rolling(window=250).max().iloc[-1]
        if close < (high_250 * 0.85):
            continue
            
        # 5. 近90日涨幅 >= 25% (粗略取 63 个交易日)
        price_90d_ago = hist['Close'].iloc[-63]
        ret_90d = (close - price_90d_ago) / price_90d_ago
        if ret_90d < 0.25:
            continue
            
        # 6. RSI (14) >= 55
        delta = hist['Close'].diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=13, adjust=False).mean()
        ema_down = down.ewm(com=13, adjust=False).mean()
        rs = ema_up / ema_down
        rsi = 100 - (100 / (1 + rs)).iloc[-1]
        if rsi < 55:
            continue
            
        # 7. 量比 (Volume Ratio) >= 1.5
        avg_vol_50 = hist['Volume'].rolling(window=50).mean().iloc[-1]
        vol_ratio = volume / avg_vol_50
        if vol_ratio < 1.5:
            continue
            
        # 8. 短期涨幅 (用于机构排序)
        ret_1d = (close - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]
        ret_5d = (close - hist['Close'].iloc[-6]) / hist['Close'].iloc[-6]
        ret_20d = (close - hist['Close'].iloc[-21]) / hist['Close'].iloc[-21]

        # 记录符合条件的股票
        final_stocks.append({
            "Ticker": ticker,
            "Price": round(close, 2),
            "1D%": f"{round(ret_1d * 100, 2)}%",
            "5D%": f"{round(ret_5d * 100, 2)}%",
            "20D%": f"{round(ret_20d * 100, 2)}%", # 用于“按近20日涨幅排序”
            "Volume_Ratio": round(vol_ratio, 2),
            "Turnover(M)": round(turnover / 1000000, 2),
            "RSI": round(rsi, 2),
            "90D_Return%": f"{round(ret_90d * 100, 2)}%",
            "Struct": "Strong (MA20>MA50)"
        })
        print(f"发现符合特征股票: {ticker}")
        
    except Exception as e:
        # 忽略获取数据报错的个股
        continue

# ==========================================
# 4. 排序并写入 Google Sheets
# ==========================================
if final_stocks:
    df = pd.DataFrame(final_stocks)
    # 按照机构交易员技巧：按 近20日涨幅 排序 (降序)
    df['20D_Num'] = df['20D%'].str.replace('%', '').astype(float)
    df = df.sort_values(by='20D_Num', ascending=False).drop(columns=['20D_Num'])
    
    # 清空旧数据
    sheet.clear()
    
    # 写入表头和数据
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    
    # 添加更新时间记录
    now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet.update_cell(1, len(df.columns) + 2, "Last Updated:")
    sheet.update_cell(1, len(df.columns) + 3, now_time)
    
    print(f"成功将 {len(df)} 只股票写入 Google 表格！")
else:
    print("今天没有符合欧奈尔形态的股票。")
