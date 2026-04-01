import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import time
import math

# 屏蔽干扰
warnings.filterwarnings('ignore')

# ==========================================
# 1. 核心配置 (极致精简版)
# ==========================================
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_SHEET_NAME = "us Screener" 
CREDS_FILE = "credentials.json"

# 精选高动能核心池 (减少下载量，确保 10秒内完成下载)
CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "GOOGL", "AAPL", "MSFT", "META", "AMZN", "AMD", "AVGO",
    "SMCI", "ARM", "CF", "PR", "MSTR", "COIN", "NET", "SNOW", "PANW", "CRWD",
    "ON", "MU", "TQQQ", "SOXL", "LLY", "VRT", "ANET", "HOOD", "UBER", "SHOP"
]

# ==========================================
# 2. 潜龙净化与逻辑 (保留核心)
# ==========================================
def hidden_dragon_clean(val):
    if val is None or (isinstance(val, float) and not math.isfinite(val)): return ""
    if isinstance(val, (np.floating, float)): return float(round(val, 3))
    if isinstance(val, (datetime.date, datetime.datetime)): return val.strftime('%Y-%m-%d')
    return str(val)

def calculate_v1000_nexus(df, spy_last, spy_ma50):
    try:
        if len(df) < 60: return None
        close = df['Close']
        curr = close.iloc[-1]
        ma50 = close.rolling(50).mean().iloc[-1]
        
        # 紧致度 (VCP感知)
        tightness = (close.tail(10).std() / close.tail(10).mean()) * 100
        # 相对强度评分
        perf = (curr - close.iloc[-60]) / close.iloc[-60] if len(close)>60 else 0
        
        signals = []
        score = 0
        if curr > ma50 and tightness < 1.5: 
            signals.append("👁️奇點"); score += 3
        if curr > close.tail(120).max() * 0.98: 
            signals.append("🚀突破"); score += 2
            
        if score == 0: return None
        return {"Score": score, "Signals": "+".join(signals), "Price": curr, "Tight": tightness, "Perf": perf}
    except: return None

# ==========================================
# 3. 闪电执行引擎
# ==========================================
def run_fast_scan():
    start_time = time.time()
    print(f"⚡ 闪电枢纽系统启动...")

    # 1. 快速获取基准 (约 2-3秒)
    spy = yf.download("SPY", period="150d", progress=False, threads=False)
    spy_last = spy['Close'].iloc[-1]
    spy_ma50 = spy['Close'].rolling(50).mean().iloc[-1]
    
    # 2. 批量下载核心池 (约 5-8秒)
    data = yf.download(CORE_TICKERS, period="150d", group_by='ticker', threads=True, progress=False)
    
    candidates = []
    for t in CORE_TICKERS:
        try:
            df_t = data[t].dropna()
            res = calculate_v1000_nexus(df_t, spy_last, spy_ma50)
            if res:
                res["Ticker"] = t
                candidates.append(res)
        except: continue

    # 3. 排序并构建结果 (约 1秒)
    results = []
    if candidates:
        final_df = pd.DataFrame(candidates).sort_values("Score", ascending=False).head(10)
        for _, r in final_df.iterrows():
            results.append({
                "Ticker": r['Ticker'], "评级": "🔥强势" if r['Score'] >= 3 else "✅关注",
                "信号": r['Signals'], "Price": r['Price'], "紧致度": f"{round(r['Tight'],2)}%",
                "季度涨幅": f"{round(r['Perf']*100,1)}%"
            })

    # 4. 极速同步 (约 3-5秒)
    sync_to_sheets(results, spy_last > spy_ma50)
    
    print(f"🏁 任务完成！总耗时: {round(time.time() - start_time, 2)} 秒")

def sync_to_sheets(results, healthy):
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID).worksheet(TARGET_SHEET_NAME)
        
        bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
        
        header = [
            ["⚡ V1000 闪电版", "状态:", "🔥活跃" if healthy else "❄️冷静", "Time:", bj_now],
            ["Ticker", "评级", "信号", "Price", "紧致度", "季度涨幅"]
        ]
        
        body = [[hidden_dragon_clean(r[k]) for k in header[1]] for r in results] if results else [["无信号"]]
        
        # 一次性清空并写入 (最快路径)
        sh.clear()
        sh.update('A1', header + body)
        print("📝 Sheets 同步成功")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_fast_scan()
