import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import time
import os
from polygon import RESTClient

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "你的_POLYGON_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)

# Google 表格 ID (确保已共享权限给 service account)
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json"

# 行业板块映射 (个股归属锁定)
SECTOR_MAP = {
    "SMH": ["NVDA", "AMD", "TSM", "AVGO", "ARM", "ASML", "MU", "INTC", "AMAT", "JBL"],
    "XLK": ["AAPL", "MSFT", "ORCL", "CRM", "VRT", "PLTR", "PANW", "SNOW"],
    "XLF": ["JPM", "BAC", "GS", "MS", "WFC", "V", "MA", "PCAR"],
    "XLI": ["CAT", "LMT", "GE", "HON", "UPS", "STLD", "NUE", "DOV", "LHX"],
    "XLY": ["AMZN", "TSLA", "HD", "NKE", "MAR", "BKNG"],
    "XBI": ["REGN", "VRTX", "AMGN", "GILD"],
    "XLV": ["UNH", "LLY", "JNJ", "PFE", "ABBV", "CAH"]
}

MONITOR_ETFS = ["SPY", "QQQ", "IWM", "SMH", "XLK", "XLF", "XLI", "XBI", "XLY"]

# ==========================================
# 2. 鲁棒性标的抓取函数 (解决 KeyError: 'Ticker')
# ==========================================
def get_robust_us_tickers():
    headers = {'User-Agent': 'Mozilla/5.0'}
    tickers = []
    
    # --- 抓取 S&P 500 ---
    try:
        url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url_sp, storage_options=headers)
        for df in tables:
            if 'Symbol' in df.columns:
                tickers.extend(df['Symbol'].tolist())
                break
    except Exception as e: print(f"⚠️ S&P 500 抓取失败: {e}")

    # --- 抓取 Nasdaq-100 ---
    try:
        url_ndx = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        tables = pd.read_html(url_ndx, storage_options=headers)
        for df in tables:
            # 自动识别列名：可能是 Ticker 或 Symbol
            possible_cols = ['Ticker', 'Symbol', 'Ticker symbol']
            target_col = next((c for c in possible_cols if c in df.columns), None)
            
            if target_col and len(df) > 90: # 纳指100应该有100行左右，过滤掉小表
                tickers.extend(df[target_col].tolist())
                break
    except Exception as e: print(f"⚠️ Nasdaq-100 抓取失败: {e}")

    # 清洗数据：去重、转字符串、处理 .B 之类的符号
    clean = list(set([str(t).strip().replace('.', '-') for t in tickers if isinstance(t, (str, float)) and str(t) != 'nan']))
    return clean

# ==========================================
# 3. 量化核心算法 (POC & Option Flow)
# ==========================================
def calculate_poc(df, bins=50):
    if len(df) < 60: return 0, 0
    lookback = df.tail(120)
    p_min, p_max = lookback['Low'].min(), lookback['High'].max()
    if p_max == p_min: return 0, 0
    counts, bin_edges = np.histogram(lookback['Close'], bins=bins, weights=lookback['Volume'])
    max_idx = np.argmax(counts)
    poc_price = (bin_edges[max_idx] + bin_edges[max_idx+1]) / 2
    dist_to_poc = (df['Close'].iloc[-1] - poc_price) / poc_price
    return round(poc_price, 2), dist_to_poc

def get_option_sentiment(ticker, is_etf=False):
    """
    Polygon 异动探测逻辑：Vol > OI 代表新大单入场
    """
    try:
        snapshots = client_poly.get_snapshot_options_chain(ticker)
        bull_val, total_val = 0, 0
        threshold = 200000 if is_etf else 25000 
        
        for s in snapshots:
            vol = s.day.volume
            oi = s.open_interest if s.open_interest else 0
            # 核心异动判断
            if vol > 50 and vol > oi:
                notional = vol * (s.day.last if s.day.last else 0) * 100
                if notional > threshold:
                    total_val += notional
                    if s.details.contract_type == 'call':
                        bull_val += notional
        if total_val == 0: return 50, "Neutral"
        score = round((bull_val / total_val) * 100, 2)
        return score, f"${round(total_val/1e6, 2)}M({score}%)"
    except: return 50, "NoData/Limit"

# ==========================================
# 4. 主扫描引擎 (全维度共振)
# ==========================================
def run_pro_scanner():
    # A. 扫描 ETF 水温
    print("📡 [1/3] 正在扫描全板块 ETF 期权异动...")
    etf_sentiments = {}
    for etf in MONITOR_ETFS:
        score, desc = get_option_sentiment(etf, is_etf=True)
        etf_sentiments[etf] = {"score": score, "desc": desc}
        time.sleep(1) # 频率保护

    # B. 获取个股并扫描技术面
    tickers = get_robust_us_tickers()
    if not tickers: return
    
    print(f"🚀 [2/3] 扫描 {len(tickers)} 只个股技术面 (动量+POC支撑)...")
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
    
    final_results = []
    # QQQ/SPY 大盘平均得分
    market_score = (etf_sentiments["SPY"]["score"] + etf_sentiments["QQQ"]["score"]) / 2

    for ticker in tickers:
        try:
            if ticker not in data.columns.levels[0]: continue
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            close = float(df['Close'].iloc[-1])
            if close < 15: continue
            
            # 策略：多头趋势 + 回调支撑位 (距离POC支撑在 -1.5% 到 7% 之间)
            ma50, ma200 = df['Close'].tail(50).mean(), df['Close'].tail(200).mean()
            ret_120d = (close - df['Close'].iloc[-126]) / df['Close'].iloc[-126]
            poc_p, dist_poc = calculate_poc(df)
            
            is_match = (ma50 > ma200) and (ret_120d > 0.18) and (dist_poc >= -0.015 and dist_poc <= 0.07)
            
            if is_match:
                # 寻找该股所属行业情绪
                sector, s_score = "None", 50
                for s_etf, s_list in SECTOR_MAP.items():
                    if ticker in s_list:
                        sector, s_score = s_etf, etf_sentiments[s_etf]['score']
                        break
                
                print(f"🎯 技术命中: {ticker} (所属板块: {sector})，获取期权数据...")
                time.sleep(12.5) # Polygon 免费版频率控制
                stock_score, stock_desc = get_option_sentiment(ticker)
                
                # 评级逻辑：共振程度
                rating = "⚡ 技术回调"
                if stock_score > 60: rating = "🔥 个股异动"
                if stock_score > 60 and s_score > 55: rating = "💎 板块共振"
                if stock_score > 60 and s_score > 55 and market_score > 50: rating = "🚀 SSS级共振"

                final_results.append({
                    "Ticker": ticker,
                    "Rating": rating,
                    "Sector": sector,
                    "Stock_Opt": stock_score,
                    "Sector_Opt": s_score,
                    "Market_Opt": round(market_score, 1),
                    "Price": close,
                    "Dist_POC": f"{round(dist_poc*100, 1)}%",
                    "Opt_Detail": stock_desc
                })
        except: continue

    # D. 同步
    output_to_sheets(final_results, etf_sentiments)

# ==========================================
# 5. Google Sheets 同步
# ==========================================
def output_to_sheets(results, etfs):
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)
        sheet = sh.worksheet("Screener")
        sheet.clear()

        # 看板写入
        etf_header = [["板块 ETF 情绪监控看板 (机构资金流)"]]
        etf_header.append(["ETF", "看涨比例", "异动描述"])
        for k, v in etfs.items():
            etf_header.append([k, f"{v['score']}%", v['desc']])
        sheet.update(values=etf_header, range_name="A1")

        if results:
            df = pd.DataFrame(results).sort_values(by=['Stock_Opt', 'Sector_Opt'], ascending=False)
            start_row = len(etf_header) + 4
            sheet.update(values=[["=== 策略精选 (动量龙头 + POC支撑 + 期权异动共振) ==="]], range_name=f"A{start_row-1}")
            sheet.update(values=[df.columns.tolist()] + df.values.tolist(), range_name=f"A{start_row}")
        
        sheet.update_acell("I1", f"Last Update: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"✅ 选股完成！同步 {len(results)} 只个股。")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_pro_scanner()
