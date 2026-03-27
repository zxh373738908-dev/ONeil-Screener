import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
from polygon import RESTClient

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "YOUR_POLYGON_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)

# Google Sheets 配置
SHEET_URL = "你的表格URL"
creds = Credentials.from_service_account_file("credentials.json", 
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
client_gs = gspread.authorize(creds)

# 核心监控的 ETF 列表 (大盘 + 关键行业)
MONITOR_ETFS = ["SPY", "QQQ", "IWM", "XLK", "XLF", "XLV", "SMH"]

# ==========================================
# 2. 筹码峰计算函数
# ==========================================
def calculate_poc(df, bins=50):
    if len(df) < 60: return 0, 0
    lookback_df = df.tail(120)
    p_min, p_max = lookback_df['Low'].min(), lookback_df['High'].max()
    if p_max == p_min: return 0, 0
    counts, bin_edges = np.histogram(lookback_df['Close'], bins=bins, weights=lookback_df['Volume'])
    max_idx = np.argmax(counts)
    poc_price = (bin_edges[max_idx] + bin_edges[max_idx+1]) / 2
    current_price = df['Close'].iloc[-1]
    dist_to_poc = (current_price - poc_price) / poc_price
    return round(poc_price, 2), dist_to_poc

# ==========================================
# 3. Polygon 期权异动探测器 (增强版)
# ==========================================
def get_options_flow_sentiment(ticker, is_etf=False):
    """
    is_etf: ETF 的资金量更大，过滤阈值需调高
    """
    try:
        snapshots = client_poly.get_snapshot_options_chain(ticker)
        bullish_value = 0
        total_value = 0
        
        # 阈值设置：ETF 大单需 > 10万美金，个股 > 2万美金
        threshold = 100000 if is_etf else 20000
        
        for s in snapshots:
            vol = s.day.volume
            oi = s.open_interest if s.open_interest else 0
            last_p = s.day.last if s.day.last else 0
            
            # Unusual 核心逻辑：Vol > OI (新开仓)
            if vol > 100 and vol > (oi * 1.1):
                notional_value = vol * last_p * 100
                if notional_value > threshold:
                    total_value += notional_value
                    if s.details.contract_type == 'call':
                        bullish_value += notional_value
        
        if total_value == 0: return 50, "Neutral/No Flow"
        
        sentiment = round((bullish_value / total_value) * 100, 2)
        desc = f"${round(total_value/1e6, 2)}M | Bull:{sentiment}%"
        return sentiment, desc
    except:
        return 50, "API Error"

# ==========================================
# 4. 扫描引擎
# ==========================================
def run_enhanced_scanner():
    # A. 先获取 ETF 的现状 (作为背景过滤)
    print("📡 正在扫描核心 ETF 期权风向...")
    etf_signals = {}
    etf_data = yf.download(MONITOR_ETFS, period="1y", group_by='ticker', progress=False)
    
    for etf in MONITOR_ETFS:
        df_etf = etf_data[etf].dropna()
        poc, dist = calculate_poc(df_etf)
        sent, desc = get_options_flow_sentiment(etf, is_etf=True)
        # 记录 ETF 是否在支撑位且期权看涨
        etf_signals[etf] = {
            "at_support": (dist >= -0.02 and dist <= 0.05),
            "bullish_flow": (sent > 55),
            "desc": desc
        }
    
    # B. 获取个股列表
    headers = {'User-Agent': 'Mozilla/5.0'}
    sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].tolist()
    ndx100 = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)[0]['Ticker'].tolist()
    tickers = list(set([str(t).replace('.', '-') for t in (sp500 + ndx100)]))
    
    print(f"🚀 开始扫描 {len(tickers)} 只个股技术面...")
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
    
    final_picks = []
    for ticker in tickers:
        try:
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            close = float(df['Close'].iloc[-1])
            # 基础动量 + 筹码支撑 策略
            poc_p, dist_poc = calculate_poc(df)
            ma50, ma200 = df['Close'].tail(50).mean(), df['Close'].tail(200).mean()
            ret_120d = (close - df['Close'].iloc[-126]) / df['Close'].iloc[-126]
            rsi = 100 - (100 / (1 + (df['Close'].tail(14).diff().clip(lower=0).mean() / -df['Close'].tail(14).diff().clip(upper=0).mean())))

            # 策略核心条件
            is_strategy_match = (ret_120d > 0.15) and (ma50 > ma200) and \
                                (dist_poc >= -0.015 and dist_poc <= 0.06) and (rsi < 62)

            if is_strategy_match:
                # C. 命中技术面后，调入 Polygon 期权探测器
                opt_sent, opt_desc = get_options_flow_sentiment(ticker)
                
                # 如果个股期权看涨情绪 > 60%，则是高胜率标的
                if opt_sent >= 60:
                    # 确定所属行业 (简单演示：如果是科技股，看 QQQ 信号)
                    # 实际操作中可增加判断逻辑，此处演示普适逻辑
                    mkt_context = "Strong" if etf_signals["SPY"]["bullish_flow"] else "Weak"
                    
                    final_picks.append({
                        "Ticker": ticker,
                        "Opt_Score": opt_sent,
                        "Mkt_Context": mkt_context,
                        "Price": close,
                        "Dist_POC": f"{round(dist_poc*100,2)}%",
                        "Opt_Flow": opt_desc,
                        "Mom_120d": f"{round(ret_120d*100,2)}%"
                    })
                    print(f"🎯 发现高胜率标的: {ticker} (期权评分: {opt_sent})")
        except:
            continue

    # D. 写入 Google Sheets
    update_sheets(final_picks, etf_signals)

def update_sheets(picks, etf_signals):
    try:
        sheet = client_gs.open_by_url(SHEET_URL).worksheet("Screener")
        sheet.clear()
        
        # 先写 ETF 状态作为参考
        etf_df = pd.DataFrame([{"ETF": k, "Flow": v["desc"], "Support": v["at_support"]} for k, v in etf_signals.items()])
        sheet.update(values=[["--- 大盘/行业风向标 ---"]] + [etf_df.columns.tolist()] + etf_df.values.tolist(), range_name="A1")
        
        # 再写选股结果
        if picks:
            df = pd.DataFrame(picks).sort_values(by='Opt_Score', ascending=False)
            start_row = len(etf_df) + 4
            sheet.update(values=[["--- 策略精选个股 (动量+筹码+期权异动) ---"]] + [df.columns.tolist()] + df.values.tolist(), 
                         range_name=f"A{start_row}")
        
        print("✅ Sheets 更新完成")
    except Exception as e:
        print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_enhanced_scanner()
