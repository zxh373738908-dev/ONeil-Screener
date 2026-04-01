import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import math
from polygon import RESTClient

# ==========================================
# 1. 配置中心 (填入你的 API KEY)
# ==========================================
POLYGON_API_KEY = "您的_POLYGON_API_KEY"
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyVyClySDklAPrx30URWZOGyb423Vb_5Dzt7WCKQ6WJwcNb7HLqwD0ckiMYm5sTwnLz/exec"

client_poly = RESTClient(POLYGON_API_KEY)

# 扫描池：你可以自由增加标的，30只以内通常在10秒内完成
CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "CF", "PR", "GOOGL", "AAPL", "MSFT", 
    "META", "AMZN", "AMD", "COIN", "SMCI", "AVGO", "LLY", "VRT", "ANET", "HOOD"
]

# 模拟板块分类 (手动映射以保证速度)
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

        # 1. 相对强度 (RS)
        rs_line = close / spy_df
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max()
        
        # 2. 紧致度 (VCP感知)
        tightness = (close.tail(10).std() / close.tail(10).mean()) * 100
        
        # 3. RS 性能评分 (加权计算)
        def get_perf(d): return (curr_price - close.iloc[-d]) / close.iloc[-d]
        rs_score = (get_perf(63)*2 + get_perf(126) + get_perf(252))

        signals, base_res = [], 0
        if rs_nh_20 and tightness < 1.4: signals.append("👁️奇點"); base_res += 3
        if curr_price >= high.tail(252).max() * 0.98 and vol.iloc[-1] > vol_ma50: signals.append("🚀突破"); base_res += 2
        if rs_score > 0.4 and abs(curr_price - ma50)/ma50 < 0.04: signals.append("🐉回頭"); base_res += 2

        if not signals and rs_score < 0.2: return None
        
        adr = ((high - low) / low).tail(20).mean() * 100
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr
        }
    except: return None

def get_option_audit(ticker):
    """期权哨兵：感知暗盘流向"""
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        c_val, p_val = 0, 0
        for s in snaps[:80]: # 快速扫描前80个主力合约
            v = s.day.volume if s.day else 0
            if v < 50: continue
            val = v * (s.day.last or 0) * 100
            if s.details.contract_type == 'call': c_val += val
            else: p_val += val
        return f"{round(c_val / (c_val + p_val + 1) * 100, 1)}%"
    except: return "N/A"

# ==========================================
# 3. 主指挥流程
# ==========================================
def run_v1000_final():
    start_time = time.time()
    print("🏟️ V1000 终极枢纽系统启动...")

    # 1. 数据下载
    data = yf.download(CORE_TICKERS, period="1y", group_by='ticker', threads=True, progress=False)
    spy_df = yf.download("SPY", period="1y", progress=False)['Close'].dropna()
    vix = yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1]

    candidates = []
    sector_cluster = {}
    
    # 2. 核心演算
    for t in CORE_TICKERS:
        try:
            res = calculate_v1000_nexus(data[t].dropna(), spy_df)
            if res:
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP.get(t, "其他")
                candidates.append(res)
                # 统计板块异动数
                if res["Base_Res"] > 0:
                    sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 全市场进入静默期。")
        return

    # 3. 集群加成与审计
    final_list = []
    # 按得分和 RS 排序
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(12)
    
    print(f"🔥 发现 {len(sorted_df)} 只候选标的，执行深度审计...")
    for i, row in sorted_df.reset_index().iterrows():
        # 板块集群加成
        cluster_count = sector_cluster.get(row['Sector'], 1)
        total_score = row['Base_Res'] + (1 if cluster_count >= 2 else 0)
        
        # 期权审计 (仅对前 2 名，防止 Polygon 报错)
        opt_call = "N/A"
        if i < 2 and total_score >= 3:
            opt_call = get_option_audit(row['Ticker'])
            time.sleep(0.5)

        rating = "💎SSS 共振" if total_score >= 5 else "🔥强势" if total_score >= 3 else "✅观察"
        
        final_list.append([
            row['Ticker'],
            rating,
            " + ".join(row['Signals']) if row['Signals'] else "趋势保持",
            f"{cluster_count}只异动",
            opt_call,
            round(row['Price'], 2),
            f"{round(row['Tightness'], 2)}%",
            round(row['RS_Score'], 2),
            f"{round(row['ADR'], 2)}%",
            row['Sector']
        ])

    # 4. 同步至 Google Sheets (固定 10 列)
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🏰 V1000 终极枢纽", "Update:", bj_now, "VIX:", round(vix, 2), "", "", "", "", ""],
        ["代码", "评级", "枢纽信号", "板块集群", "看涨% (Top2)", "现价", "紧致度", "RS强度", "ADR", "板块"]
    ]
    
    matrix = header + final_list
    
    try:
        requests.post(WEBAPP_URL, data=json.dumps(matrix), headers={'Content-Type': 'application/json'}, timeout=15)
        print(f"🎉 任务达成！耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v1000_final()
