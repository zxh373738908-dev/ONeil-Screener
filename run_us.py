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
# 1. 配置中心 (请填入你的 KEY)
# ==========================================
POLYGON_API_KEY = "你的_POLYGON_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)

# Google Sheets 配置
SHEET_URL = "你的_GOOGLE_SHEET_URL"
creds_file = "credentials.json"  # 确保此文件在目录下

# 核心监控 ETF
MONITOR_ETFS = ["SPY", "QQQ", "IWM", "XLK", "SMH", "XBI"]

# ==========================================
# 2. 股票池获取 (修复 Wikipedia KeyError)
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
            if 'Ticker' in df.columns:
                tickers.extend(df['Ticker'].tolist())
                break
            elif 'Symbol' in df.columns:
                tickers.extend(df['Symbol'].tolist())
                break
    except Exception as e: print(f"⚠️ Nasdaq-100 抓取失败: {e}")

    # 清洗数据 (去重、格式化)
    clean_tickers = list(set([str(t).strip().replace('.', '-') for t in tickers if isinstance(t, str)]))
    print(f"✅ 成功获取 {len(clean_tickers)} 只核心个股标的")
    return clean_tickers

# ==========================================
# 3. 筹码峰 (POC) 核心算法
# ==========================================
def calculate_poc(df, bins=50):
    if len(df) < 60: return 0, 0
    lookback = df.tail(120)  # 分析过去半年筹码分布
    p_min, p_max = lookback['Low'].min(), lookback['High'].max()
    if p_max == p_min: return 0, 0
    
    # 使用直方图计算成交量分布
    counts, bin_edges = np.histogram(lookback['Close'], bins=bins, weights=lookback['Volume'])
    max_idx = np.argmax(counts)
    poc_price = (bin_edges[max_idx] + bin_edges[max_idx+1]) / 2
    
    current_price = df['Close'].iloc[-1]
    dist_to_poc = (current_price - poc_price) / poc_price
    return round(poc_price, 2), dist_to_poc

# ==========================================
# 4. Polygon 期权异动探测 (增强胜率)
# ==========================================
def get_option_sentiment(ticker, is_etf=False):
    """
    通过 Polygon API 扫描期权链
    逻辑: 寻找当日成交量 > 持仓量 (Unusual) 的大金额订单
    """
    try:
        snapshots = client_poly.get_snapshot_options_chain(ticker)
        bull_val, total_val, unusual_cnt = 0, 0, 0
        
        # 阈值：ETF 10万美金，个股 3万美金 (过滤小散户)
        threshold = 100000 if is_etf else 30000
        
        for s in snapshots:
            vol = s.day.volume
            oi = s.open_interest if s.open_interest else 0
            price = s.day.last if s.day.last else 0
            
            # 核心异动逻辑: Vol > OI (新资金入场)
            if vol > 100 and vol > (oi * 1.1):
                notional = vol * price * 100
                if notional > threshold:
                    unusual_cnt += 1
                    total_val += notional
                    if s.details.contract_type == 'call':
                        bull_val += notional
        
        if total_val == 0: return 50, "No Flow"
        sentiment = round((bull_val / total_val) * 100, 2)
        desc = f"${round(total_val/1e6, 2)}M | Bull:{sentiment}%"
        return sentiment, desc
    except Exception:
        return 50, "API Error"

# ==========================================
# 5. 主扫描逻辑
# ==========================================
def run_scanner():
    # 1. 初始化
    tickers = get_us_core_tickers()
    if not tickers: return
    
    # 2. 扫描 ETF 背景
    print("📡 正在分析大盘 ETF 期权情绪...")
    etf_summary = []
    etf_data = yf.download(MONITOR_ETFS, period="1y", group_by='ticker', progress=False)
    for etf in MONITOR_ETFS:
        sent, desc = get_option_sentiment(etf, is_etf=True)
        etf_summary.append({"ETF": etf, "Sentiment": sent, "Detail": desc})
        time.sleep(1) # 规避 API 限制

    # 3. 下载并扫描个股
    print(f"🚀 启动量化引擎扫描 {len(tickers)} 只个股...")
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
    
    final_picks = []
    
    for ticker in tickers:
        try:
            if ticker not in data.columns.levels[0]: continue
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            close = float(df['Close'].iloc[-1])
            vol_daily = float(df['Volume'].iloc[-1])
            
            # 基础过滤：价格 > 15 且 日成交额 > 5000万美金
            if close < 15 or (close * vol_daily) < 50000000: continue
            
            # 指标计算
            ma50, ma200 = df['Close'].tail(50).mean(), df['Close'].tail(200).mean()
            ret_120d = (close - df['Close'].iloc[-126]) / df['Close'].iloc[-126]
            poc_p, dist_poc = calculate_poc(df)
            
            # RSI
            delta = df['Close'].tail(14).diff()
            up, down = delta.clip(lower=0).mean(), -delta.clip(upper=0).mean()
            rsi = 100 - (100 / (1 + (up/down if down != 0 else 1)))

            # 策略：多头趋势 + 动量超 20% + 筹码支撑位 (0%-7%) + RSI 不超买
            is_match = (ma50 > ma200) and (ret_120d > 0.20) and \
                       (dist_poc >= -0.01 and dist_poc <= 0.07) and (rsi < 62)

            if is_match:
                print(f"🔍 捕捉到技术面信号: {ticker}，正在请求 Polygon 期权数据...")
                
                # 针对命中标的，调用期权 API (加入频率控制)
                time.sleep(12) 
                opt_sent, opt_flow = get_option_sentiment(ticker)
                
                # 期权过滤：只有当看涨金额占比 > 58% 时确认为高胜率
                if opt_sent >= 58:
                    final_picks.append({
                        "Ticker": ticker,
                        "Opt_Score": opt_sent,
                        "Price": close,
                        "Dist_POC": f"{round(dist_poc*100, 2)}%",
                        "Mom_120d": f"{round(ret_120d*100, 2)}%",
                        "RSI": round(rsi, 2),
                        "Option_Flow": opt_flow
                    })
                    print(f"⭐ 策略共振命中: {ticker} | 期权评分: {opt_sent}")

        except Exception: continue

    # 4. 同步至 Google Sheets
    output_to_sheets(final_picks, etf_summary)

# ==========================================
# 6. Google Sheets 同步函数 (增强报错版)
# ==========================================
def output_to_sheets(picks, etf_sum):
    try:
        print("开始连接 Google Sheets...")
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        
        if not os.path.exists(creds_file):
            print(f"❌ 错误: 找不到凭据文件 {creds_file}")
            return

        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        client = gspread.authorize(creds)
        
        try:
            sh = client.open_by_url(SHEET_URL)
        except Exception as e:
            print(f"❌ 无法打开表格。原因: {repr(e)}")
            print("💡 提示: 请确保你已将 credentials.json 中的 client_email 添加为表格的‘编辑器’。")
            return

        try:
            sheet = sh.worksheet("Screener")
        except:
            sheet = sh.add_worksheet(title="Screener", rows="100", cols="20")

        sheet.clear()
        
        # 1. 写入背景
        df_etf = pd.DataFrame(etf_sum)
        sheet.update(values=[["=== 大盘/行业期权背景 (辅助参考) ==="]] + [df_etf.columns.tolist()] + df_etf.values.tolist(), range_name="A1")
        
        # 2. 写入个股结果
        if picks:
            df_picks = pd.DataFrame(picks).sort_values(by='Opt_Score', ascending=False)
            start_row = len(df_etf) + 5
            sheet.update(values=[["=== 策略精选: 动量+筹码支撑+期权扫单 ==="]] + [df_picks.columns.tolist()] + df_picks.values.tolist(), 
                         range_name=f"A{start_row}")
        
        # 更新时间
        now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        sheet.update(values=[["Last Run:", now_str]], range_name="I1")
        print(f"✅ 成功！已同步 {len(picks)} 只个股到 Google Sheets。")

    except Exception as e:
        print(f"❌ Sheets 同步致命错误: {repr(e)}")

if __name__ == "__main__":
    run_scanner()
