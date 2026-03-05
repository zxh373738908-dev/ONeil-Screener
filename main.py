import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=913399386#gid=913399386" # ⚠️ 记得换成你的真实链接！

scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. 获取股票池 (美股标普+纳指，A股沪深300)
# ==========================================
def get_us_tickers():
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].tolist()
        ndx100 = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)[4]['Ticker'].tolist()
        tickers = list(set(sp500 + ndx100))
        return[t.replace('.', '-') for t in tickers]
    except Exception as e:
        print(f"获取美股代码失败: {e}")
        return[]

def get_a_share_tickers():
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        tables = pd.read_html('https://en.wikipedia.org/wiki/CSI_300_Index', storage_options=headers)
        # 智能寻找包含 Ticker 和 Exchange 的表格
        for tbl in tables:
            if any(col in tbl.columns for col in ['Ticker', 'Symbol']) and any(col in tbl.columns for col in['Exchange', 'Stock exchange']):
                ticker_col = 'Ticker' if 'Ticker' in tbl.columns else 'Symbol'
                exchange_col = 'Exchange' if 'Exchange' in tbl.columns else 'Stock exchange'
                
                tickers =[]
                for _, row in tbl.iterrows():
                    # A股代码补齐6位
                    ticker = str(row[ticker_col]).replace('.0', '').zfill(6)
                    exchange = str(row[exchange_col]).lower()
                    # 匹配雅虎财经的后缀: 上海 .SS, 深圳 .SZ
                    if 'shanghai' in exchange:
                        tickers.append(f"{ticker}.SS")
                    elif 'shenzhen' in exchange:
                        tickers.append(f"{ticker}.SZ")
                
                if len(tickers) > 100:
                    return list(set(tickers))
        return[]
    except Exception as e:
        print(f"获取沪深300代码失败: {e}")
        # 如果维基百科改版导致抓取失败，提供一份A股核心资产保底列表，防止程序崩溃
        return['600519.SS', '300750.SZ', '002594.SZ', '601318.SS', '600036.SS', '000858.SZ', '600276.SS', '000333.SZ', '600900.SS', '002415.SZ']

# ==========================================
# 3. 核心筛选逻辑 (复用模块，中美通用)
# ==========================================
def screen_stocks(tickers, min_price, min_turnover):
    final_stocks =[]
    print(f"开始筛选 {len(tickers)} 只股票...")
    
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1y")
            
            if len(hist) < 200: 
                continue
                
            close = hist['Close'].iloc[-1]
            volume = hist['Volume'].iloc[-1]
            
            # 1. 过滤价格与缺失值
            if pd.isna(close) or pd.isna(volume) or close < min_price:
                continue
                
            # 2. 过滤成交额 (美股美元，A股人民币)
            turnover = close * volume
            if turnover < min_turnover:
                continue
                
            # 3. 欧奈尔核心均线: MA20 > MA50 且 价格 > MA200
            ma20 = hist['Close'].rolling(window=20).mean().iloc[-1]
            ma50 = hist['Close'].rolling(window=50).mean().iloc[-1]
            ma200 = hist['Close'].rolling(window=200).mean().iloc[-1]
            
            if not (close > ma20 and ma20 > ma50 and close > ma200):
                continue
                
            # 4. 突破位置：接近 250日最高价的 85%
            high_250 = hist['High'].rolling(window=250).max().iloc[-1]
            if pd.isna(high_250):
                high_250 = hist['High'].max()
                
            if close < (high_250 * 0.85):
                continue
                
            # 5. 动量：近 90日涨幅 >= 25%
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
                
            # 7. 量比 >= 1.5
            avg_vol_50 = hist['Volume'].rolling(window=50).mean().iloc[-1]
            vol_ratio = volume / avg_vol_50 if avg_vol_50 > 0 else 0
            if vol_ratio < 1.5:
                continue
                
            # 计算近期涨幅用于排序
            ret_1d = (close - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]
            ret_5d = (close - hist['Close'].iloc[-6]) / hist['Close'].iloc[-6]
            ret_20d = (close - hist['Close'].iloc[-21]) / hist['Close'].iloc[-21]

            final_stocks.append({
                "Ticker": ticker,
                "Price": round(close, 2),
                "1D%": f"{round(ret_1d * 100, 2)}%",
                "5D%": f"{round(ret_5d * 100, 2)}%",
                "20D%": f"{round(ret_20d * 100, 2)}%", 
                "Volume_Ratio": round(vol_ratio, 2),
                "Turnover(M)": round(turnover / 1000000, 2),
                "RSI": round(rsi, 2),
                "90D_Return%": f"{round(ret_90d * 100, 2)}%",
                "Struct": "Strong (MA20>MA50)"
            })
            print(f"✅ 发现符合特征股票: {ticker}")
            
        except Exception as e:
            continue
            
    return final_stocks

# ==========================================
# 4. 写入 Google Sheets 指定工作表
# ==========================================
def write_to_sheet(sheet_name, final_stocks):
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            # 按 近20日涨幅 降序排列
            df['20D_Num'] = df['20D%'].str.replace('%', '').astype(float)
            df = df.sort_values(by='20D_Num', ascending=False).drop(columns=['20D_Num'])
            
            data_to_write =[df.columns.values.tolist()] + df.values.tolist()
            
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_cell(1, len(df.columns) + 2, "Last Updated:")
            sheet.update_cell(1, len(df.columns) + 3, now_time)
            
            print(f"🎉 成功将 {len(df)} 只股票写入 {sheet_name}！")
        else:
            sheet.clear()
            sheet.update_acell("A1", "今天没有符合欧奈尔条件的股票")
            print(f"⚠️ {sheet_name}: 今天没有符合条件的股票。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

# ==========================================
# 5. 执行主程序
# ==========================================
if __name__ == "__main__":
    print("========== 开始处理美股 ==========")
    us_tickers = get_us_tickers()
    # 美股参数：价格>15美元，成交额>5000万美元
    us_results = screen_stocks(us_tickers, min_price=15, min_turnover=50000000)
    write_to_sheet("Screener", us_results)
    
    print("\n========== 开始处理A股 (沪深300) ==========")
    a_tickers = get_a_share_tickers()
    # A股参数：价格>5元人民币，成交额>1亿人民币
    a_results = screen_stocks(a_tickers, min_price=5, min_turnover=100000000)
    write_to_sheet("A-Share Screener", a_results)
