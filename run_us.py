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

# 忽略警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "你的_POLYGON_API_KEY" 
client_poly = RESTClient(POLYGON_API_KEY)

# 目标表格 ID
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json" 

MONITOR_ETFS = ["SPY", "QQQ", "IWM", "XLK", "SMH"]

# ==========================================
# 2. 股票池获取
# ==========================================
def get_us_core_tickers():
    headers = {'User-Agent': 'Mozilla/5.0'}
    tickers = []
    try:
        url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url_sp, storage_options=headers)
        for df in tables:
            if 'Symbol' in df.columns:
                tickers.extend(df['Symbol'].tolist())
                break
    except: pass
    try:
        url_ndx = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        tables = pd.read_html(url_ndx, storage_options=headers)
        for df in tables:
            if 'Ticker' in df.columns or 'Symbol' in df.columns:
                col = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
                tickers.extend(df[col].tolist())
                break
    except: pass
    return list(set([str(t).strip().replace('.', '-') for t in tickers if isinstance(t, str)]))

# ==========================================
# 3. 筹码峰 (POC) 计算
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

# ==========================================
# 4. Polygon 期权探测 (阈值调低至 $10,000 以增加灵敏度)
# ==========================================
def get_option_sentiment(ticker, is_etf=False):
    try:
        snapshots = client_poly.get_snapshot_options_chain(ticker)
        bullish_val, total_val = 0, 0
        # 调低阈值：ETF 5万，个股 1万
        threshold = 50000 if is_etf else 10000
        
        for s in snapshots:
            vol = s.day.volume
            oi = s.open_interest if s.open_interest else 0
            price = s.day.last if s.day.last else 0
            
            # Unusual 核心判断: 日内成交 > 持仓 (新开仓)
            if vol > 50 and vol > oi:
                notional = vol * price * 100
                if notional > threshold:
                    total_val += notional
                    if s.details.contract_type == 'call':
                        bullish_val += notional
        
        if total_val == 0: return 50, "无显著异动"
        sentiment = round((bullish_val / total_val) * 100, 2)
        desc = f"${round(total_val/1000, 1)}K | Bull:{sentiment}%"
        return sentiment, desc
    except:
        return 50, "数据接口限制"

# ==========================================
# 5. 主扫描逻辑 (取消硬性期权过滤)
# ==========================================
def run_scanner():
    tickers = get_us_core_tickers()
    if not tickers: return
    
    print("📡 分析大盘 ETF 期权背景...")
    etf_summary = []
    etf_data = yf.download(MONITOR_ETFS, period="1y", group_by='ticker', progress=False)
    for etf in MONITOR_ETFS:
        sent, desc = get_option_sentiment(etf, is_etf=True)
        etf_summary.append({"ETF": etf, "Sentiment": sent, "Detail": desc})
        time.sleep(1)

    print(f"🚀 扫描 {len(tickers)} 只个股技术面与筹码支撑...")
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
    
    final_picks = []
    for ticker in tickers:
        try:
            if ticker not in data.columns.levels[0]: continue
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            close = float(df['Close'].iloc[-1])
            if close < 10: continue
            
            ma50, ma200 = df['Close'].tail(50).mean(), df['Close'].tail(200).mean()
            ret_120d = (close - df['Close'].iloc[-126]) / df['Close'].iloc[-126]
            poc_p, dist_poc = calculate_poc(df)
            
            # 技术面：多头趋势 + 动量 > 15% + 靠近筹码峰 (-1.5% 到 +8%)
            is_match = (ret_120d > 0.15) and (ma50 > ma200) and \
                       (dist_poc >= -0.015 and dist_poc <= 0.08)

            if is_match:
                print(f"🎯 命中技术支撑: {ticker}，正在获取期权数据...")
                # 加入 12.5 秒延时，防止免费版 API 报错
                time.sleep(12.5) 
                opt_sent, opt_flow = get_option_sentiment(ticker)
                
                # --- 核心改进：不再过滤，全部记录，但标记评分 ---
                final_picks.append({
                    "Ticker": ticker,
                    "Signal_Strength": "🔥🔥🔥 高胜率" if opt_sent > 58 else "⚡ 技术面回调",
                    "Opt_Score": opt_sent,
                    "Price": close,
                    "Dist_POC": f"{round(dist_poc*100, 1)}%",
                    "120D_Return": f"{round(ret_120d*100, 1)}%",
                    "Option_Detail": opt_flow
                })

        except: continue

    output_to_sheets(final_picks, etf_summary)

# ==========================================
# 6. Google Sheets 同步
# ==========================================
def output_to_sheets(picks, etf_sum):
    try:
        print("开始同步数据至 Google Sheets...")
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)

        try:
            sheet = sh.worksheet("Screener")
        except:
            sheet = sh.add_worksheet(title="Screener", rows="100", cols="20")

        sheet.clear()
        
        # 背景数据
        df_etf = pd.DataFrame(etf_sum)
        sheet.update(values=[["=== 大盘期权风向标 ==="]] + [df_etf.columns.tolist()] + df_etf.values.tolist(), range_name="A1")
        
        # 选股数据
        if picks:
            # 优先排序：期权情绪高的排在前面
            df_picks = pd.DataFrame(picks).sort_values(by=['Opt_Score'], ascending=False)
            start_row = len(df_etf) + 5
            sheet.update(values=[["=== 策略精选 (动量龙头 + 筹码支撑 + 期权异动) ==="]] + [df_picks.columns.tolist()] + df_picks.values.tolist(), 
                         range_name=f"A{start_row}")
        
        last_run = f"Last Updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
        sheet.update(values=[[last_run]], range_name="I1")
        print(f"✅ 同步完成！共计 {len(picks)} 只个股录入表格。")

    except Exception as e:
        print(f"❌ Sheets 同步失败: {repr(e)}")

if __name__ == "__main__":
    run_scanner()
