import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import requests
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "你的_GOOGLE_SCRIPT_URL" # 填入你的URL

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL", "ARM", "MU", "TSM"
]

def get_performance(series, days):
    if len(series) < days + 1: return 0
    return ((series.iloc[-1] / series.iloc[-(days+1)]) - 1) * 100

# ==========================================
# 2. 机构级逻辑引擎 V14.0
# ==========================================
def calculate_v14_logic(ticker_obj, df, spy_df):
    try:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = df.index.tz_localize(None)
        df = df.dropna(subset=['Close'])
        
        if len(df) < 65: return None
        
        close = df['Close'].astype(float)
        high = df['High'].astype(float)
        low = df['Low'].astype(float)
        volume = df['Volume'].astype(float)
        curr_price = float(close.iloc[-1])
        
        # 1. 基础指标
        ma20 = close.rolling(window=20).mean()
        ma20_curr = ma20.iloc[-1]
        ema10 = close.ewm(span=10, adjust=False).mean().iloc[-1]
        sma50 = close.rolling(window=50).mean().iloc[-1]
        
        # 2. 乖离与量比
        ext = ((curr_price - ma20_curr) / ma20_curr) * 100
        vol_ratio = volume.iloc[-1] / volume.tail(20).mean()
        adr = ((high - low) / low).tail(20).mean() * 100

        # 3. 涨幅计算
        p5d = get_performance(close, 5)
        p20d = get_performance(close, 20)
        p60d = get_performance(close, 60)
        
        # 4. 相对强度 R20/R60 (vs SPY)
        spy_close = spy_df.reindex(close.index).ffill()
        spy_20p = get_performance(spy_close, 20)
        spy_60p = get_performance(spy_close, 60)
        r20 = p20d - spy_20p
        r60 = p60d - spy_60p

        # 5. 趋势与评分
        is_s2 = curr_price > ema10 > ma20_curr > sma50
        score = 0
        if is_s2: score += 3
        if curr_price > close.iloc[-2]: score += 1
        if r20 > 0: score += 1
        if vol_ratio > 1.2: score += 1
        
        # 6. 决策逻辑
        action = "WAIT"
        if score >= 5: action = "STRONG BUY"
        elif score >= 3: action = "HOLD/ADD"
        elif curr_price < ma20_curr: action = "REDUCE"
        
        # 7. 共振信号
        resonance = "No"
        if (curr_price > ema10) and (vol_ratio > 1) and (r20 > 0):
            resonance = "🔥TRIPLE"

        # 8. 获取Info (行业/市值)
        info = ticker_obj.info
        industry = info.get('industry', 'N/A')
        mkt_cap = info.get('marketCap', 0) / 1e9 # 十亿美元
        
        return {
            "Industry": industry,
            "Score": score,
            "Action": action,
            "Resonance": resonance,
            "ADR": round(adr, 2),
            "Vol_Ratio": round(vol_ratio, 2),
            "Bias": round(ext, 2),
            "MktCap": f"{mkt_cap:.1f}B",
            "RS_Rank": round(score * 16.6, 1),
            "Options": "Yes" if info.get('optionsExpirationDates') else "No",
            "Price": round(curr_price, 2),
            "5D": f"{p5d:.2f}%",
            "20D": f"{p20d:.2f}%",
            "60D": f"{p60d:.2f}%",
            "R20": round(r20, 2),
            "R60": round(r60, 2)
        }
    except Exception as e:
        print(f"Error logic: {e}")
        return None

# ==========================================
# 3. 主程序
# ==========================================
def run_v14_engine():
    print(f"🚀 V14.0 终端启动 | {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    spy_raw = yf.download("SPY", period="2y", progress=False)
    spy_df = spy_raw['Close'] if not isinstance(spy_raw.columns, pd.MultiIndex) else spy_raw['Close'].iloc[:, 0]

    results = []
    for t in CORE_TICKERS:
        try:
            tk = yf.Ticker(t)
            df_t = tk.history(period="2y") # 使用history保证与info同步
            res = calculate_v14_logic(tk, df_t, spy_df)
            if res:
                print(f"✅ {t} 处理完毕")
                results.append([
                    t, res['Industry'], res['Score'], res['Action'], res['Resonance'],
                    res['ADR'], res['Vol_Ratio'], res['Bias'], res['MktCap'],
                    res['RS_Rank'], res['Options'], res['Price'], 
                    res['5D'], res['20D'], res['60D'], res['R20'], res['R60']
                ])
        except Exception as e:
            print(f"❌ {t} 失败: {e}")

    # 排序 (评分 > R20)
    results.sort(key=lambda x: (x[2], x[15]), reverse=True)

    # 构造表头
    header = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
    
    # 输出或发送
    final_output = [header] + results
    
    # 打印前3行看样板
    for row in final_output[:5]:
        print(row)

    # 发送至 Google Sheets (可选)
    try:
        requests.post(WEBAPP_URL, json=final_output, timeout=15)
        print("🎉 云端同步完成")
    except:
        print("⚠️ 云端同步跳过 (未配置URL或超时)")

if __name__ == "__main__":
    run_v14_engine()
