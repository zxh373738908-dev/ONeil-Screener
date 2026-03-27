import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import time
from polygon import RESTClient

# 忽略警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请务必填写你的 API KEY)
# ==========================================
POLYGON_API_KEY = "你的_POLYGON_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)

# Google Sheets 配置
SHEET_URL = "你的_GOOGLE_SHEET_URL"
creds_file = "credentials.json" # 确保此文件在目录下

# 核心监控 ETF (用于判断大盘/行业背景)
MONITOR_ETFS = ["SPY", "QQQ", "IWM", "XLK", "SMH"]

# ==========================================
# 2. 健壮的股票列表获取 (修复 KeyError)
# ==========================================
def get_us_core_tickers():
    headers = {'User-Agent': 'Mozilla/5.0'}
    tickers = []
    
    # 抓取 S&P 500
    try:
        url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url_sp, storage_options=headers)
        for df in tables:
            if 'Symbol' in df.columns:
                tickers.extend(df['Symbol'].tolist())
                break
    except Exception as e: print(f"⚠️ S&P 500 抓取失败: {e}")

    # 抓取 Nasdaq-100
    try:
        url_ndx = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        tables = pd.read_html(url_ndx, storage_options=headers)
        for df in tables:
            # 兼容维基百科不同的列名
            if 'Ticker' in df.columns:
                tickers.extend(df['Ticker'].tolist())
                break
            elif 'Symbol' in df.columns:
                tickers.extend(df['Symbol'].tolist())
                break
    except Exception as e: print(f"⚠️ Nasdaq-100 抓取失败: {e}")

    # 清洗数据
    clean_tickers = list(set([str(t).strip().replace('.', '-') for t in tickers if isinstance(t, str)]))
    return clean_tickers

# ==========================================
# 3. 核心量化算法 (POC & 动量)
# ==========================================
def calculate_poc(df, bins=50):
    """计算过去120天的筹码最密集价格(POC)"""
    if len(df) < 60: return 0, 0
    lookback = df.tail(120)
    p_min, p_max = lookback['Low'].min(), lookback['High'].max()
    if p_max == p_min: return 0, 0
    counts, bin_edges = np.histogram(lookback['Close'], bins=bins, weights=lookback['Volume'])
    max_idx = np.argmax(counts)
    poc_price = (bin_edges[max_idx] + bin_edges[max_idx+1]) / 2
    dist_to_poc = (df['Close'].iloc[-1] - poc_price) / poc_price
    return round(poc_price, 2), dist_to_poc

# ==========================================
# 4. Polygon 期权异动探测 (增强胜率)
# ==========================================
def get_option_sentiment(ticker, is_etf=False):
    """
    通过扫描期权链判断大资金情绪
    规则: 成交量 > 持仓量 (Unusual) 且 单笔名义价值 > 阈值
    """
    try:
        # 免费版 API 建议此处加一点微小的手工延迟，防止 429 报错
        snapshots = client_poly.get_snapshot_options_chain(ticker)
        
        bull_val, total_val, unusual_cnt = 0, 0, 0
        threshold = 100000 if is_etf else 30000 # ETF 过滤阈值更高
        
        for s in snapshots:
            vol = s.day.volume
            oi = s.open_interest if s.open_interest else 0
            price = s.day.last if s.day.last else 0
            
            # Unusual 核心判断: 日内成交 > 现有持仓 (说明是今天新开的单)
            if vol > 100 and vol > oi:
                notional = vol * price * 100
                if notional > threshold:
                    unusual_cnt += 1
                    total_val += notional
                    if s.details.contract_type == 'call':
                        bull_val += notional
        
        if total_val == 0: return 50, "No Large Flow"
        sentiment = round((bull_val / total_val) * 100, 2)
        desc = f"${round(total_val/1e6, 2)}M | Bull:{sentiment}%"
        return sentiment, desc
    except Exception as e:
        return 50, f"Error: {str(e)[:15]}"

# ==========================================
# 5. 主扫描引擎
# ==========================================
def run_scanner():
    # A. 获取全市场标的
    tickers = get_us_core_tickers()
    if not tickers: return
    
    # B. 背景诊断：扫描 ETF
    print("📡 正在诊断大盘/行业 ETF 期权背景...")
    etf_summary = []
    etf_data = yf.download(MONITOR_ETFS, period="1y", group_by='ticker', progress=False)
    for etf in MONITOR_ETFS:
        sent, desc = get_option_sentiment(etf, is_etf=True)
        etf_summary.append({"ETF": etf, "Sentiment": sent, "Detail": desc})
        time.sleep(1) # 简单规避频率限制
    
    # C. 下载个股数据
    print(f"🚀 开始扫描 {len(tickers)} 只个股技术面...")
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
    
    final_picks = []
    
    # D. 循环扫描
    for ticker in tickers:
        try:
            if ticker not in data.columns.levels[0]: continue
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            # --- 策略条件计算 ---
            close = float(df['Close'].iloc[-1])
            volume = float(df['Volume'].iloc[-1])
            if close < 15 or (close * volume) < 50000000: continue # 流动性过滤
            
            ma50 = df['Close'].tail(50).mean()
            ma200 = df['Close'].tail(200).mean()
            ret_120d = (close - df['Close'].iloc[-126]) / df['Close'].iloc[-126]
            poc_p, dist_poc = calculate_poc(df)
            
            # RSI 回调计算
            delta = df['Close'].tail(14).diff()
            up, down = delta.clip(lower=0).mean(), -delta.clip(upper=0).mean()
            rsi = 100 - (100 / (1 + (up/down if down != 0 else 1)))

            # --- 核心策略：动量龙头 + 筹码支撑 + RSI不超买 ---
            is_tech_ok = (ret_120d > 0.20) and (ma50 > ma200) and \
                         (dist_poc >= -0.01 and dist_poc <= 0.07) and (rsi < 62)

            if is_tech_ok:
                print(f"🔍 技术面命中: {ticker}，正在请求 Polygon 期权数据...")
                
                # --- 胜率增强：期权异动扫描 ---
                # 注意：如果是 Polygon 免费版，此处必须 Sleep 防止 429 报错
                time.sleep(12) 
                opt_sent, opt_flow = get_option_sentiment(ticker)
                
                # 过滤条件：只有期权情绪 > 58% (偏看涨) 才入选
                if opt_sent >= 58:
                    final_picks.append({
                        "Ticker": ticker,
                        "Opt_Score": opt_sent,
                        "Mom_120d": f"{round(ret_120d*100, 2)}%",
                        "Price": close,
                        "Dist_POC": f"{round(dist_poc*100, 2)}%",
                        "RSI": round(rsi, 2),
                        "Option_Flow": opt_flow
                    })
                    print(f"⭐ 发现高胜率标的: {ticker} (期权看涨比例: {opt_sent}%)")

        except Exception as e:
            continue

    # E. 写入 Google Sheets
    output_to_sheets(final_picks, etf_summary)

def output_to_sheets(picks, etf_sum):
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(SHEET_URL).worksheet("Screener")
        sheet.clear()
        
        # 1. 写入 ETF 背景
        df_etf = pd.DataFrame(etf_sum)
        sheet.update(values=[["=== 大盘/行业期权风向 ==="]] + [df_etf.columns.tolist()] + df_etf.values.tolist(), range_name="A1")
        
        # 2. 写入个股结果
        if picks:
            df_picks = pd.DataFrame(picks).sort_values(by='Opt_Score', ascending=False)
            start_row = len(df_etf) + 4
            sheet.update(values=[["=== 策略精选 (动量+筹码+期权多头共振) ==="]] + [df_picks.columns.tolist()] + df_picks.values.tolist(), 
                         range_name=f"A{start_row}")
        
        sheet.update_acell("I1", f"Last Run: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("✅ 选股完成，结果已同步至 Google Sheets。")
    except Exception as e:
        print(f"❌ Sheets 同步失败: {e}")

if __name__ == "__main__":
    run_scanner()
