import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time  # <--- 修复此处的导入
import requests
import json
import math
import warnings
from polygon import RESTClient

# 屏蔽干扰
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请确保填入你的 Key 和 URL)
# ==========================================
POLYGON_API_KEY = "您的_POLYGON_API_KEY"
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyVyClySDklAPrx30URWZOGyb423Vb_5Dzt7WCKQ6WJwcNb7HLqwD0ckiMYm5sTwnLz/exec"

client_poly = RESTClient(POLYGON_API_KEY)

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "CF", "PR", "GOOGL", "AAPL", "MSFT", 
    "META", "AMZN", "AMD", "COIN", "SMCI", "AVGO", "LLY", "VRT", "ANET", "HOOD"
]

SECTOR_MAP = {
    "NVDA": "AI/半导体", "AMD": "AI/半导体", "AVGO": "AI/半导体", "SMCI": "AI/半导体",
    "TSLA": "新能源", "PLTR": "软件/AI", "MSFT": "软件/AI", "GOOGL": "软件/AI",
    "MSTR": "加密货币", "COIN": "加密货币", "MARA": "加密货币", "HOOD": "加密货币",
    "CF": "化肥/资源", "PR": "化肥/资源", "AAPL": "消费电子", "META": "社交/AI",
    "AMZN": "电商/云", "VRT": "基础设施", "ANET": "基础设施"
}

# ==========================================
# 2. 核心算法逻辑
# ==========================================
def calculate_v1000_nexus(df, spy_df):
    try:
        if len(df) < 150: return None
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = close.iloc[-1]
        ma50 = close.rolling(50).mean().iloc[-1]
        vol_ma50 = vol.rolling(50).mean().iloc[-1]

        rs_line = close / spy_df
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max()
        tightness = (close.tail(10).std() / close.tail(10).mean()) * 100
        
        def get_perf(d): return (curr_price - close.iloc[-d]) / close.iloc[-d]
        rs_score = (get_perf(63)*2.5 + get_perf(126)*1.5 + get_perf(250))

        signals, base_res = [], 0
        if rs_nh_20:
            if tightness < 1.6:
                signals.append("👁️奇點")
                base_res += 4
            else:
                signals.append("📈趋势")
                base_res += 1
                
        high_52w = high.tail(252).max()
        if curr_price >= high_52w * 0.97:
            if vol.iloc[-1] > vol_ma50:
                signals.append("🚀突破")
                base_res += 3
            else:
                signals.append("🔭临界")
                base_res += 1
                
        if rs_score > 0.4 and abs(curr_price - ma50)/ma50 < 0.04:
            signals.append("🐉回頭")
            base_res += 2

        if rs_score < -0.1: return None 
        
        adr = ((high - low) / low).tail(20).mean() * 100
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr
        }
    except: return None

def get_option_audit(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        c_val, p_val = 0, 0
        for s in snaps[:80]:
            v = s.day.volume if s.day else 0
            if v < 50: continue
            val = v * (s.day.last or 0) * 100
            if s.details.contract_type == 'call': c_val += val
            else: p_val += val
        return f"{round(c_val / (c_val + p_val + 1) * 100, 1)}%"
    except: return "N/A"

# ==========================================
# 3. 主指挥引擎
# ==========================================
def run_v1000_final():
    start_time = time.time()
    print("🚀 V1000 枢纽系统 [9.2完整版] 启动...")

    try:
        data = yf.download(CORE_TICKERS, period="1y", group_by='ticker', threads=True, progress=False)
        spy_df = yf.download("SPY", period="1y", progress=False)['Close'].dropna()
        vix_df = yf.download("^VIX", period="5d", progress=False)['Close']
        vix = vix_df.iloc[-1] if not vix_df.empty else 20.0
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    candidates = []
    sector_cluster = {}
    
    for t in CORE_TICKERS:
        try:
            df_t = data[t].dropna()
            if df_t.empty: continue
            res = calculate_v1000_nexus(df_t, spy_df)
            if res:
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP.get(t, "其他")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 未发现符合多头条件的标的。")
        return

    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(12)
    final_list = []
    
    print(f"🔥 处理 {len(sorted_df)} 只多头候选...")
    for i, row in sorted_df.reset_index().iterrows():
        cluster_count = sector_cluster.get(row['Sector'], 1)
        total_score = row['Base_Res'] + (1 if cluster_count >= 2 else 0)
        
        opt_call = "N/A"
        if i < 2 and total_score >= 1:
            opt_call = get_option_audit(row['Ticker'])
            time.sleep(0.5)

        if total_score >= 5: rating = "💎SSS 共振"
        elif total_score >= 3: rating = "🔥强势"
        elif row['RS_Score'] > 0.5: rating = "🚀高动能"
        else: rating = "✅监控"
        
        final_list.append([
            row['Ticker'],
            rating,
            " + ".join(row['Signals']) if row['Signals'] else "📊 蓄势中",
            f"{cluster_count}只异动",
            opt_call,
            round(row['Price'], 2),
            f"{round(row['Tightness'], 2)}%",
            round(row['RS_Score'], 2),
            f"{round(row['ADR'], 2)}%",
            row['Sector']
        ])

    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🏰 V1000 终极枢纽 (9.2版)", "Update:", bj_now, "VIX:", round(vix, 2), "", "", "", "", ""],
        ["代码", "评级", "枢纽信号", "板块集群", "看涨% (Top2)", "现价", "紧致度", "RS强度", "ADR", "板块"]
    ]
    
    try:
        requests.post(WEBAPP_URL, data=json.dumps(header + final_list), headers={'Content-Type': 'application/json'}, timeout=15)
        print(f"🎉 同步完成！耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v1000_final()
