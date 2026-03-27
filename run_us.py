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
# 1. 配置中心 (请填入你的 Polygon API Key)
# ==========================================
POLYGON_API_KEY = "你的_POLYGON_API_KEY" 
client_poly = RESTClient(POLYGON_API_KEY)

# 目标表格 ID (已根据您提供的链接提取)
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json" 

# 监控的大盘风向标
MONITOR_ETFS = ["SPY", "QQQ", "IWM", "XLK", "SMH"]

# ==========================================
# 2. 股票池获取 (S&P 500 + Nasdaq 100)
# ==========================================
def get_us_core_tickers():
    headers = {'User-Agent': 'Mozilla/5.0'}
    tickers = []
    
    # 获取 S&P 500
    try:
        url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url_sp, storage_options=headers)
        for df in tables:
            if 'Symbol' in df.columns:
                tickers.extend(df['Symbol'].tolist())
                break
    except Exception as e: print(f"⚠️ S&P 500 抓取失败: {e}")

    # 获取 Nasdaq-100
    try:
        url_ndx = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        tables = pd.read_html(url_ndx, storage_options=headers)
        for df in tables:
            if 'Ticker' in df.columns or 'Symbol' in df.columns:
                col = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
                tickers.extend(df[col].tolist())
                break
    except Exception as e: print(f"⚠️ Nasdaq-100 抓取失败: {e}")

    clean_tickers = list(set([str(t).strip().replace('.', '-') for t in tickers if isinstance(t, str)]))
    print(f"✅ 成功加载 {len(clean_tickers)} 只核心个股标的")
    return clean_tickers

# ==========================================
# 3. 筹码峰 (POC) 计算
# ==========================================
def calculate_poc(df, bins=50):
    if len(df) < 60: return 0, 0
    lookback = df.tail(120) # 过去 120 个交易日
    p_min, p_max = lookback['Low'].min(), lookback['High'].max()
    if p_max == p_min: return 0, 0
    
    counts, bin_edges = np.histogram(lookback['Close'], bins=bins, weights=lookback['Volume'])
    max_idx = np.argmax(counts)
    poc_price = (bin_edges[max_idx] + bin_edges[max_idx+1]) / 2
    
    dist_to_poc = (df['Close'].iloc[-1] - poc_price) / poc_price
    return round(poc_price, 2), dist_to_poc

# ==========================================
# 4. Polygon 期权探测 (Unusual Option Activity)
# ==========================================
def get_option_sentiment(ticker, is_etf=False):
    try:
        # 获取期权链当日快照
        snapshots = client_poly.get_snapshot_options_chain(ticker)
        bullish_val, total_val = 0, 0
        threshold = 100000 if is_etf else 30000
        
        for s in snapshots:
            vol = s.day.volume
            oi = s.open_interest if s.open_interest else 0
            price = s.day.last if s.day.last else 0
            
            # Unusual Logic: 当日成交 > 现有持仓 (说明是机构今天刚开的仓)
            if vol > 100 and vol > oi:
                notional = vol * price * 100
                if notional > threshold:
                    total_val += notional
                    if s.details.contract_type == 'call':
                        bullish_val += notional
        
        if total_val == 0: return 50, "No Flow"
        sentiment = round((bullish_val / total_val) * 100, 2)
        desc = f"${round(total_val/1e6, 2)}M | Bull:{sentiment}%"
        return sentiment, desc
    except Exception:
        return 50, "API Limit/Error"

# ==========================================
# 5. 主扫描逻辑
# ==========================================
def run_scanner():
    tickers = get_us_core_tickers()
    if not tickers: return
    
    # A. 扫描背景 ETF 情绪
    print("📡 正在分析大盘 ETF 期权情绪...")
    etf_summary = []
    etf_data = yf.download(MONITOR_ETFS, period="1y", group_by='ticker', progress=False)
    for etf in MONITOR_ETFS:
        sent, desc = get_option_sentiment(etf, is_etf=True)
        etf_summary.append({"ETF": etf, "Sentiment": sent, "Detail": desc})
        time.sleep(1) # 频率保护

    # B. 全市场技术面海选
    print(f"🚀 开始扫描 {len(tickers)} 只个股技术面与筹码峰...")
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
    
    final_picks = []
    for ticker in tickers:
        try:
            if ticker not in data.columns.levels[0]: continue
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            close = float(df['Close'].iloc[-1])
            vol_daily = float(df['Volume'].iloc[-1])
            if close < 15 or (close * vol_daily) < 50000000: continue
            
            # 指标计算
            ma50, ma200 = df['Close'].tail(50).mean(), df['Close'].tail(200).mean()
            ret_120d = (close - df['Close'].iloc[-126]) / df['Close'].iloc[-126]
            poc_p, dist_poc = calculate_poc(df)
            
            # --- 核心策略过滤 ---
            # 1. 中期强势动量 (涨幅 > 20%)
            # 2. 长期趋势向上 (MA50 > MA200)
            # 3. 价格处于筹码支撑位 (POC 价格上方 0% - 7% 之间)
            is_match = (ret_120d > 0.20) and (ma50 > ma200) and \
                       (dist_poc >= -0.01 and dist_poc <= 0.07)

            if is_match:
                print(f"🔍 捕捉到技术回调支撑: {ticker}，正在获取期权异动数据...")
                # Polygon 免费版限速，每 12.5 秒查询一只，确保稳定
                time.sleep(12.5) 
                opt_sent, opt_flow = get_option_sentiment(ticker)
                
                # 期权过滤：只有看涨情绪高于 58% 才录入，代表“真抄底”
                if opt_sent >= 58:
                    final_picks.append({
                        "Ticker": ticker,
                        "Opt_Score": opt_sent,
                        "Price": close,
                        "POC_Support": poc_p,
                        "Dist_POC": f"{round(dist_poc*100, 2)}%",
                        "120D_Return": f"{round(ret_120d*100, 2)}%",
                        "Option_Detail": opt_flow
                    })
                    print(f"⭐ 策略命中: {ticker} | 情绪评分: {opt_sent} | 筹码支撑: {poc_p}")

        except Exception: continue

    # C. 同步至 Google Sheets
    output_to_sheets(final_picks, etf_summary)

# ==========================================
# 6. Google Sheets 同步 (使用 open_by_key)
# ==========================================
def output_to_sheets(picks, etf_sum):
    try:
        print("开始同步数据至 Google Sheets...")
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        
        if not os.path.exists(creds_file):
            print(f"❌ 找不到凭据文件: {creds_file}")
            return

        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        client = gspread.authorize(creds)
        
        # 使用 open_by_key 直接打开，规避 URL 格式问题
        sh = client.open_by_key(SHEET_ID)

        try:
            sheet = sh.worksheet("Screener")
        except:
            sheet = sh.add_worksheet(title="Screener", rows="100", cols="20")

        sheet.clear()
        
        # 写入大盘背景
        df_etf = pd.DataFrame(etf_sum)
        sheet.update(values=[["=== 大盘期权背景风向标 ==="]] + [df_etf.columns.tolist()] + df_etf.values.tolist(), range_name="A1")
        
        # 写入个股精选结果
        if picks:
            df_picks = pd.DataFrame(picks).sort_values(by='Opt_Score', ascending=False)
            start_row = len(df_etf) + 5
            header = [["=== 策略精选 (动量龙头 + 筹码支撑 + 期权扫单) ==="]]
            sheet.update(values=header + [df_picks.columns.tolist()] + df_picks.values.tolist(), range_name=f"A{start_row}")
        
        # 写入运行时间
        last_run = f"Last Updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
        sheet.update(values=[[last_run]], range_name="I1")
        print(f"✅ 同步完成！共计 {len(picks)} 只个股录入表格。")

    except Exception as e:
        print(f"❌ Sheets 同步致命错误: {repr(e)}")

if __name__ == "__main__":
    run_scanner()
