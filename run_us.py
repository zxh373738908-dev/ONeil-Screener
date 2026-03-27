import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import time
from polygon import RESTClient

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "你的_POLYGON_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)

SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json"

# 行业板块映射表 (帮助个股锁定所属行业情绪)
SECTOR_MAP = {
    "SMH": ["NVDA", "AMD", "TSM", "AVGO", "ARM", "ASML", "MU", "INTC", "AMAT", "JBL"],
    "XLK": ["AAPL", "MSFT", "ORCL", "CRM", "VRT", "PLTR", "PANW", "SNOW"],
    "XLF": ["JPM", "BAC", "GS", "MS", "WFC", "V", "MA", "PCAR"],
    "XLI": ["CAT", "LMT", "GE", "HON", "UPS", "STLD", "NUE", "DOV", "LHX"],
    "XLY": ["AMZN", "TSLA", "HD", "NKE", "MAR", "BKNG"],
    "XBI": ["REGN", "VRTX", "AMGN", "GILD"],
    "XLV": ["UNH", "LLY", "JNJ", "PFE", "ABBV", "CAH"]
}

# 需要监测的所有 ETF
MONITOR_ETFS = ["SPY", "QQQ", "IWM", "SMH", "XLK", "XLF", "XLI", "XBI", "XLY"]

# ==========================================
# 2. 核心算法：POC & 期权流
# ==========================================
def calculate_poc(df, bins=50):
    if len(df) < 60: return 0, 0
    lookback = df.tail(120)
    counts, bin_edges = np.histogram(lookback['Close'], bins=bins, weights=lookback['Volume'])
    max_idx = np.argmax(counts)
    poc_price = (bin_edges[max_idx] + bin_edges[max_idx+1]) / 2
    dist_to_poc = (df['Close'].iloc[-1] - poc_price) / poc_price
    return round(poc_price, 2), dist_to_poc

def get_option_sentiment(ticker, is_etf=False):
    try:
        snapshots = client_poly.get_snapshot_options_chain(ticker)
        bullish_val, total_val = 0, 0
        threshold = 250000 if is_etf else 30000 # ETF 门槛更高
        
        for s in snapshots:
            vol = s.day.volume
            oi = s.open_interest if s.open_interest else 0
            if vol > 100 and vol > (oi * 1.1):
                notional = vol * (s.day.last if s.day.last else 0) * 100
                if notional > threshold:
                    total_val += notional
                    if s.details.contract_type == 'call':
                        bullish_val += notional
        if total_val == 0: return 50, "Neutral"
        score = round((bullish_val / total_val) * 100, 2)
        return score, f"${round(total_val/1e6, 1)}M({score}%)"
    except: return 50, "Error"

# ==========================================
# 3. 主扫描引擎
# ==========================================
def run_pro_scanner():
    # A. 扫描所有监控 ETF 的“水温”
    print("📡 [1/3] 正在扫描全板块 ETF 期权异动...")
    etf_sentiments = {}
    for etf in MONITOR_ETFS:
        score, desc = get_option_sentiment(etf, is_etf=True)
        etf_sentiments[etf] = {"score": score, "desc": desc}
        time.sleep(1) # 频率保护

    # B. 获取个股列表并下载数据
    headers = {'User-Agent': 'Mozilla/5.0'}
    sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].tolist()
    ndx100 = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)[0]['Ticker'].tolist()
    tickers = list(set([str(t).replace('.', '-') for t in (sp500 + ndx100)]))
    
    print(f"🚀 [2/3] 扫描 {len(tickers)} 只个股技术面 (动量+POC)...")
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
    
    final_results = []
    for ticker in tickers:
        try:
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            close = float(df['Close'].iloc[-1])
            if close < 15: continue
            
            # --- 核心策略逻辑 ---
            ma50, ma200 = df['Close'].tail(50).mean(), df['Close'].tail(200).mean()
            ret_120d = (close - df['Close'].iloc[-126]) / df['Close'].iloc[-126]
            poc_p, dist_poc = calculate_poc(df)
            
            # 策略：多头排列 + 强动量 + 回踩支撑 (POC支撑位)
            is_match = (ma50 > ma200) and (ret_120d > 0.18) and (dist_poc >= -0.01 and dist_poc <= 0.08)
            
            if is_match:
                # C. 深度确认：寻找该股所属的行业 ETF 情绪
                belonging_sector = "None"
                sector_score = 50
                for etf, stocks in SECTOR_MAP.items():
                    if ticker in stocks:
                        belonging_sector = etf
                        sector_score = etf_sentiments[etf]['score']
                        break
                
                print(f"🎯 技术面命中: {ticker} (所属板块: {belonging_sector})，正在请求个股期权...")
                time.sleep(12.5) # Polygon 免费版限速
                stock_score, stock_desc = get_option_sentiment(ticker)
                
                # --- 胜率评级逻辑 ---
                # 只有当：大盘(QQQ/SPY)稳健 + 行业(Sector)看涨 + 个股(Stock)看涨，才是 SSS 级
                market_score = (etf_sentiments["SPY"]["score"] + etf_sentiments["QQQ"]["score"]) / 2
                
                rating = "⚡ 技术回调"
                if stock_score > 60: rating = "🔥 个股异动"
                if stock_score > 60 and sector_score > 55: rating = "💎 板块共振"
                if stock_score > 60 and sector_score > 55 and market_score > 50: rating = "🚀 SSS级共振"

                final_results.append({
                    "Ticker": ticker,
                    "Rating": rating,
                    "Sector": belonging_sector,
                    "Stock_Opt": stock_score,
                    "Sector_Opt": sector_score,
                    "Market_Opt": round(market_score, 1),
                    "Price": close,
                    "Dist_POC": f"{round(dist_poc*100, 1)}%",
                    "Opt_Detail": stock_desc
                })
        except: continue

    # D. 同步至 Google Sheets
    output_to_sheets(final_results, etf_sentiments)

def output_to_sheets(results, etfs):
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)
        sheet = sh.worksheet("Screener")
        sheet.clear()

        # 1. 写入 ETF 全板块热力图
        etf_header = [["板块 ETF 情绪监控看板"]]
        etf_header.append(["ETF", "看涨比例", "异动描述"])
        for k, v in etfs.items():
            etf_header.append([k, f"{v['score']}%", v['desc']])
        sheet.update(values=etf_header, range_name="A1")

        # 2. 写入个股精选
        if results:
            df = pd.DataFrame(results).sort_values(by=['Stock_Opt', 'Sector_Opt'], ascending=False)
            start_row = len(etf_header) + 4
            sheet.update(values=[["=== 策略精选 (动量+筹码+多维度期权共振) ==="]], range_name=f"A{start_row-1}")
            sheet.update(values=[df.columns.tolist()] + df.values.tolist(), range_name=f"A{start_row}")
        
        print(f"✅ 成功同步 {len(results)} 只个股！")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_pro_scanner()
