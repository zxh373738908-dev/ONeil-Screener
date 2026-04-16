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
# 1. 配置中心 (V1000 10.0 Alpha)
# ==========================================
POLYGON_API_KEY = "您的_POLYGON_API_KEY"
WEBAPP_URL = "您的_G_SHEET_WEBAPP_URL" # 替换为您的 URL

client_poly = RESTClient(POLYGON_API_KEY)

# 扩展监控池（涵盖高波动的 AI、加密、能源、科技）
CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "CF", "PR", "GOOGL", "AAPL", "MSFT", 
    "META", "AMZN", "AMD", "COIN", "SMCI", "AVGO", "LLY", "VRT", "ANET", 
    "HOOD", "MARA", "RIOT", "ARM", "APP", "SOFI", "GME"
]

SECTOR_MAP = {
    "NVDA": "AI/半导体", "AMD": "AI/半导体", "AVGO": "AI/半导体", "SMCI": "AI/半导体", "ARM": "AI/半导体",
    "TSLA": "新能源", "PLTR": "软件/AI", "MSFT": "软件/AI", "GOOGL": "软件/AI", "APP": "软件/AI",
    "MSTR": "加密货币", "COIN": "加密货币", "MARA": "加密货币", "RIOT": "加密货币", "HOOD": "加密货币",
    "CF": "化肥/资源", "PR": "化肥/资源", "AAPL": "消费电子", "META": "社交/AI",
    "AMZN": "电商/云", "VRT": "基础设施", "ANET": "基础设施", "SOFI": "FinTech", "GME": "Meme/Retail"
}

# ==========================================
# 2. 反应堆核心算法
# ==========================================
def calculate_v1000_alpha_reactor(df, spy_df):
    try:
        if len(df) < 100: return None
        
        close = df['Close']
        high = df['High']
        low = df['Low']
        curr_price = float(close.iloc[-1])
        
        # --- 1. ADR 过滤 (20日平均真实波幅 %) ---
        adr = float(((high - low) / low).tail(20).mean() * 100)
        if adr < 3.5: return None  # 剔除波动率不足的“死鱼”
        
        # --- 2. 绝对多头排列 (Trend Align) ---
        ema10 = close.ewm(span=10, adjust=False).mean().iloc[-1]
        ma20 = close.rolling(window=20).mean().iloc[-1]
        sma50 = close.rolling(window=50).mean().iloc[-1]
        trend_ok = curr_price > ema10 > ma20 > sma50
        
        # --- 3. 相对强度 (RS) 强化版 ---
        spy_aligned = spy_df.reindex(close.index).ffill()
        rs_line = (close / spy_aligned).dropna()
        # 20日RS新高判断
        rs_nh_20 = bool(rs_line.iloc[-1] >= rs_line.tail(20).max())
        
        def get_perf(d): 
            if len(close) < d: return 0.0
            return float((curr_price - close.iloc[-d]) / close.iloc[-d])
        
        # RS Score: 权重 3:2:1 (3月, 6月, 12月)
        rs_score = float(get_perf(63)*3 + get_perf(126)*2 + get_perf(250))

        # --- 4. 信号矩阵 ---
        signals = []
        base_res = 0
        
        if rs_nh_20: 
            signals.append("📡RS新高")
            base_res += 3
        if trend_ok:
            signals.append("🔥主升浪")
            base_res += 3
        if curr_price >= float(high.tail(20).max()) * 0.98:
            signals.append("🚀即突破")
            base_res += 2
        
        # 紧致度 (不再作为硬指标，仅作辅助参考)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        return {
            "RS_Score": rs_score, 
            "Signals": signals, 
            "Base_Res": base_res, 
            "Price": curr_price, 
            "Tightness": tightness, 
            "ADR": adr,
            "Trend_Align": "✅多头" if trend_ok else "❌破位"
        }
    except Exception as e:
        return None

# ==========================================
# 3. 主指挥引擎
# ==========================================
def run_v1000_alpha_reactor():
    start_time = time.time()
    print("☢️ V1000 10.0 Alpha 反应堆启动...")

    try:
        # 获取大盘背景
        data = yf.download(CORE_TICKERS, period="2y", group_by='ticker', threads=True, progress=False)
        spy_df = yf.download("SPY", period="2y", progress=False)['Close'].dropna()
        vix_raw = yf.download("^VIX", period="5d", progress=False)['Close']
        vix = float(vix_raw.iloc[-1]) if not vix_raw.empty else 20.0
    except Exception as e:
        print(f"❌ 数据源故障: {e}"); return

    candidates = []
    sector_cluster = {}
    
    for t in CORE_TICKERS:
        try:
            df_t = data[t].dropna()
            if df_t.empty: continue
            
            res = calculate_v1000_alpha_reactor(df_t, spy_df)
            if res:
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP.get(t, "其他")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 当前无满足 ADR > 3.5% 且形态合格的标的"); return

    # 排序：优先排 Trend_Align 为多头的，然后看 Base_Res，最后看 RS_Score
    sorted_df = pd.DataFrame(candidates)
    sorted_df = sorted_df.sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(15)
    
    final_list = []
    for _, row in sorted_df.iterrows():
        # 评分分级
        if row['Base_Res'] >= 6:
            rating = "💎SSS 反应堆"
        elif row['Base_Res'] >= 3:
            rating = "🔥强势领涨"
        else:
            rating = "⚡观察"

        sig_str = " + ".join(row['Signals']) if row['Signals'] else "📈 维持"
        cluster = f"{sector_cluster.get(row['Sector'], 1)}只共振"
        
        final_list.append([
            str(row['Ticker']),
            rating,
            sig_str,
            cluster,
            str(row['Trend_Align']), # 新增趋势排列列
            round(float(row['Price']), 2),
            f"{round(float(row['ADR']), 2)}%", # ADR 过滤器可见
            round(float(row['RS_Score']), 2),
            f"{round(float(row['Tightness']), 2)}%",
            str(row['Sector'])
        ])

    # 4. 构造云端同步矩阵
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["☢️ V1000 Alpha 反应堆 (10.0)", "Update:", bj_now, "VIX:", round(vix, 2), "Min_ADR:", "3.5%", "", "", ""],
        ["代码", "评级", "核心信号", "板块集群", "趋势对齐", "现价", "ADR", "RS强度", "紧致度", "板块"]
    ]
    
    matrix = header + final_list
    
    try:
        payload = json.loads(json.dumps(matrix, default=lambda x: str(x)))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 反应堆同步完成！标的数: {len(final_list)} 耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v1000_alpha_reactor()
