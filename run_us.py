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

# 行业 ETF 监控列表
SECTOR_MAP = {
    "SMH": ["NVDA", "AMD", "TSM", "AVGO", "ARM", "ASML", "MU", "INTC", "AMAT", "JBL"],
    "XLK": ["AAPL", "MSFT", "ORCL", "CRM", "VRT", "PLTR", "PANW", "SNOW"],
    "XLF": ["JPM", "BAC", "GS", "MS", "WFC", "V", "MA", "PCAR"],
    "XLI": ["CAT", "LMT", "GE", "HON", "UPS", "STLD", "NUE", "DOV", "LHX"],
    "XBI": ["REGN", "VRTX", "AMGN", "GILD"],
    "XLY": ["AMZN", "TSLA", "HD", "NKE", "MAR", "BKNG"]
}
MONITOR_ETFS = ["SPY", "QQQ", "IWM", "SMH", "XLK", "XLI", "XBI"]

# ==========================================
# 2. 鲁棒性标的抓取 (解决 Wikipedia 错误)
# ==========================================
def get_robust_us_tickers():
    headers = {'User-Agent': 'Mozilla/5.0'}
    tickers = []
    try:
        sp_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(sp_url, storage_options=headers)
        for df in tables:
            if 'Symbol' in df.columns:
                tickers.extend(df['Symbol'].tolist())
                break
        ndx_url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        tables = pd.read_html(ndx_url, storage_options=headers)
        for df in tables:
            target_col = next((c for c in ['Ticker', 'Symbol'] if c in df.columns), None)
            if target_col and len(df) > 90:
                tickers.extend(df[target_col].tolist())
                break
    except: pass
    return list(set([str(t).strip().replace('.', '-') for t in tickers if str(t) != 'nan']))

# ==========================================
# 3. 核心量化算法 (RS & POC)
# ==========================================
def calculate_rs_score(tickers_data):
    """计算相对强度评分 (对比扫描池内的表现)"""
    rs_list = []
    for t, df in tickers_data.items():
        if len(df) < 252: continue
        c = df['Close']
        # 欧奈尔 RS 计算公式：(近3月涨幅*2 + 近6月 + 近9月 + 近12月)
        rs_val = ((c.iloc[-1]/c.iloc[-63])*2 + (c.iloc[-1]/c.iloc[-126]) + (c.iloc[-1]/c.iloc[-189]) + (c.iloc[-1]/c.iloc[-252]))
        rs_list.append({'Ticker': t, 'RS_Raw': rs_val})
    
    rs_df = pd.DataFrame(rs_list)
    if not rs_df.empty:
        rs_df['RS_Score'] = (rs_df['RS_Raw'].rank(pct=True) * 99).astype(int)
    return rs_df.set_index('Ticker')['RS_Score'].to_dict()

def calculate_poc(df, bins=50):
    lookback = df.tail(120)
    counts, bin_edges = np.histogram(lookback['Close'], bins=bins, weights=lookback['Volume'])
    poc_price = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
    return round(poc_price, 2)

def get_option_sentiment(ticker, is_etf=False):
    try:
        snapshots = client_poly.get_snapshot_options_chain(ticker)
        bull, total = 0, 0
        threshold = 200000 if is_etf else 25000
        for s in snapshots:
            vol, oi = s.day.volume, (s.open_interest or 0)
            if vol > 50 and vol > oi:
                val = vol * (s.day.last or 0) * 100
                if val > threshold:
                    total += val
                    if s.details.contract_type == 'call': bull += val
        if total == 0: return 50, "Neutral"
        score = round((bull / total) * 100, 1)
        return score, f"${round(total/1e6, 2)}M({score}%)"
    except: return 50, "N/A"

# ==========================================
# 4. 主扫描引擎
# ==========================================
def run_pro_scanner():
    print("📡 [1/3] 扫描板块 ETF 背景...")
    etf_sent = {e: get_option_sentiment(e, True) for e in MONITOR_ETFS}
    mkt_score = (etf_sent["SPY"][0] + etf_sent["QQQ"][0]) / 2

    tickers = get_robust_us_tickers()
    print(f"🚀 [2/3] 下载并计算 {len(tickers)} 只个股数据...")
    all_data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
    
    valid_dfs = {t: all_data[t].dropna() for t in tickers if t in all_data.columns.levels[0] and len(all_data[t].dropna()) >= 250}
    rs_map = calculate_rs_score(valid_dfs)
    
    final_results = []
    for t, df in valid_dfs.items():
        try:
            close = float(df['Close'].iloc[-1])
            vol_daily = float(df['Volume'].iloc[-1])
            avg_vol = df['Volume'].tail(50).mean()
            high_250 = df['High'].tail(252).max()
            
            # --- 筛选核心逻辑 ---
            poc_p = calculate_poc(df)
            dist_poc = (close - poc_p) / poc_p
            ret_120d = (close - df['Close'].iloc[-126]) / df['Close'].iloc[-126]
            ma50, ma200 = df['Close'].tail(50).mean(), df['Close'].tail(200).mean()
            
            # 条件：动量强劲 + 多头排列 + 靠近筹码中心
            if (ret_120d > 0.18) and (ma50 > ma200) and (-0.015 <= dist_poc <= 0.08):
                time.sleep(12.5) # Polygon 频率保护
                opt_score, opt_desc = get_option_sentiment(t)
                
                # 确定所属行业
                sector, s_score = next(((k, etf_sent[k][0]) for k, v in SECTOR_MAP.items() if t in v), ("None", 50))
                
                final_results.append({
                    "Ticker": t,
                    "Signal": "🚀SSS共振" if (opt_score > 60 and s_score > 55) else "🔥个股异动" if opt_score > 60 else "⚡技术回调",
                    "Price": round(close, 2),
                    "RS_Score": rs_map.get(t, 0),
                    "POC(筹码中心)": poc_p,
                    "上方抛压%": f"{round((high_250 - close)/close * 100, 2)}%",
                    "量比": round(vol_daily / avg_vol, 2),
                    "距高点%": f"{round((close - high_250)/high_250 * 100, 2)}%",
                    "成交额(亿)": round((close * vol_daily) / 100000000, 2),
                    "Stock_Opt": opt_score,
                    "Sector_Opt": s_score,
                    "Opt_Detail": opt_desc
                })
        except: continue

    # D. 同步至 Google Sheets
    output_to_sheets(final_results, etf_sent, mkt_score)

def output_to_sheets(results, etfs, mkt_score):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)
        sheet = sh.worksheet("Screener")
        sheet.clear()

        # 1. 顶部 ETF 看板
        etf_header = [["大盘情绪 (SPY/QQQ Avg)", f"{round(mkt_score, 1)}%", "更新时间:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')]]
        etf_header.append(["ETF", "看涨比例", "异动详情"])
        for k, v in etfs.items(): etf_header.append([k, f"{v[0]}%", v[1]])
        sheet.update(values=etf_header, range_name="A1")

        # 2. 个股精选数据
        if results:
            df = pd.DataFrame(results).sort_values(by=['Stock_Opt', 'RS_Score'], ascending=False)
            start_row = len(etf_header) + 4
            sheet.update(values=[["=== 动量龙头 + 筹码支撑 + 期权异动共振精选 ==="]], range_name=f"A{start_row-1}")
            sheet.update(values=[df.columns.tolist()] + df.values.tolist(), range_name=f"A{start_row}")
        
        print(f"✅ 完成！同步 {len(results)} 只个股。")
    except Exception as e: print(f"❌ 失败: {e}")

if __name__ == "__main__":
    run_pro_scanner()
