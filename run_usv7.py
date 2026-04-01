import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import math
import warnings
from polygon import RESTClient

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
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
# 2. 核心算法逻辑 (降门槛版)
# ==========================================
def calculate_v1000_nexus(df, spy_df):
    try:
        if len(df) < 60: return None # 只要有 60 天数据就能算
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = close.iloc[-1]
        
        # 1. 相对强度 (RS)
        rs_line = (close / spy_df.reindex(close.index).ffill()).dropna()
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max() if not rs_line.empty else False
        
        # 2. 紧致度 (VCP)
        tightness = (close.tail(10).std() / close.tail(10).mean()) * 100
        
        # 3. RS 性能评分 (加权：最近 3个月表现最重要)
        def get_perf(d): 
            if len(close) < d: return 0
            return (curr_price - close.iloc[-d]) / close.iloc[-d]
        
        rs_score = (get_perf(63)*3 + get_perf(126)*2 + get_perf(250))

        signals, base_res = [], 0
        if rs_nh_20 and tightness < 2.0: signals.append("👁️奇點"); base_res += 4
        if curr_price >= high.tail(100).max() * 0.95: signals.append("🚀高位"); base_res += 2
        
        adr = ((high - low) / low).tail(20).mean() * 100
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr
        }
    except Exception as e:
        return None

def get_option_audit(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        c_val, p_val = 0, 0
        for s in snaps[:60]:
            v = s.day.volume if s.day else 0
            if v < 30: continue
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
    print("🚀 V1000 [9.3必出结果版] 启动...")

    try:
        # 下载数据 (增加 period 以确保数据充足)
        data = yf.download(CORE_TICKERS, period="2y", group_by='ticker', threads=True, progress=False)
        spy_df = yf.download("SPY", period="2y", progress=False)['Close'].dropna()
        vix = yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1]
    except Exception as e:
        print(f"❌ 基础数据下载失败: {e}"); return

    candidates = []
    sector_cluster = {}
    
    print(f"🔍 演算中...")
    for t in CORE_TICKERS:
        try:
            # 兼容 yfinance 不同的返回格式
            df_t = data[t].dropna() if t in data.columns.levels[0] else pd.DataFrame()
            if df_t.empty or len(df_t) < 10: continue
            
            res = calculate_v1000_nexus(df_t, spy_df)
            if res:
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP.get(t, "其他")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("⚠️ 仍然没数据？正在检查下载数据...")
        print(f"下载到的代码: {list(data.columns.levels[0]) if hasattr(data.columns, 'levels') else 'None'}")
        return

    # 排序：无论有没有信号，都按 RS_Score 选出最强的 12 个
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(12)
    
    final_list = []
    print(f"✅ 找到 {len(sorted_df)} 只标的，正在同步...")
    
    for i, row in sorted_df.reset_index().iterrows():
        cluster_count = sector_cluster.get(row['Sector'], 1)
        opt_call = "N/A"
        if i < 2: # 仅审计前2名
            opt_call = get_option_audit(row['Ticker'])
            time.sleep(0.5)

        # 动态评级
        if row['Base_Res'] >= 4: rating = "💎SSS 共振"
        elif row['RS_Score'] > 0.5: rating = "🔥强势"
        else: rating = "✅监控"
        
        final_list.append([
            row['Ticker'],
            rating,
            " + ".join(row['Signals']) if row['Signals'] else "📈 趋势保持",
            f"{cluster_count}只活跃",
            opt_call,
            round(row['Price'], 2),
            f"{round(row['Tightness'], 2)}%",
            round(row['RS_Score'], 2),
            f"{round(row['ADR'], 2)}%",
            row['Sector']
        ])

    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🏰 V1000 终极枢纽 (9.3版)", "Update:", bj_now, "VIX:", round(vix, 2), "", "", "", "", ""],
        ["代码", "评级", "枢纽信号", "板块集群", "看涨% (Top2)", "现价", "紧致度", "RS强度", "ADR", "板块"]
    ]
    
    try:
        resp = requests.post(WEBAPP_URL, data=json.dumps(header + final_list), headers={'Content-Type': 'application/json'}, timeout=15)
        print(f"🎉 同步完成！耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v1000_final()
