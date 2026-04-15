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
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyVyClySDklAPrx30URWZOGyb423Vb_5Dzt7WCKQ6WJwcNb7HLqwD0ckiMYm5sTwnLz/exec"

client_poly = RESTClient(POLYGON_API_KEY)

# 扩展了更具动能的标的
CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "CF", "PR", "GOOGL", "AAPL", "MSFT", 
    "META", "AMZN", "AMD", "COIN", "SMCI", "AVGO", "LLY", "VRT", "ANET", "HOOD",
    "MARA", "CLSK", "BITF", "WBT", "GME" # 增加高ADR标的
]

SECTOR_MAP = {
    "NVDA": "AI/半导体", "AMD": "AI/半导体", "AVGO": "AI/半导体", "SMCI": "AI/半导体",
    "TSLA": "新能源", "PLTR": "软件/AI", "MSFT": "软件/AI", "GOOGL": "软件/AI",
    "MSTR": "加密货币", "COIN": "加密货币", "MARA": "加密货币", "CLSK": "加密货币",
    "CF": "化肥/资源", "PR": "化肥/资源", "AAPL": "消费电子", "META": "社交/AI",
    "AMZN": "电商/云", "VRT": "基础设施", "ANET": "基础设施", "HOOD": "金融/Crypto"
}

# ==========================================
# 2. 深度净化工具
# ==========================================
def safe_val(v, is_num=True):
    try:
        if v is None: return 0.0 if is_num else ""
        if hasattr(v, 'iloc'): v = v.iloc[-1]
        if isinstance(v, (np.floating, np.integer, float, int)):
            return float(v) if math.isfinite(v) else 0.0
        return str(v)
    except:
        return 0.0 if is_num else str(v)

# ==========================================
# 3. 核心算法逻辑 (10.0 Alpha 反应堆)
# ==========================================
def calculate_v1000_reactor(df, spy_df):
    try:
        if len(df) < 100: return None
        
        close = df['Close']
        high = df['High']
        low = df['Low']
        curr_price = float(close.iloc[-1])
        
        # --- A. ADR 烈性炸药过滤 (核心升级) ---
        # ADR = ((High - Low) / Low) 20日均值
        adr = float(((high - low) / low).tail(20).mean() * 100)
        if adr < 3.5: return None # 剔除肉股，只留妖股

        # --- B. Trend_Align 绝对多头排列 (核心升级) ---
        ema10 = close.ewm(span=10, adjust=False).mean()
        ma20 = close.rolling(window=20).mean()
        sma50 = close.rolling(window=50).mean()
        
        # 逻辑：价格 > EMA10 > MA20 > SMA50
        is_aligned = (curr_price > ema10.iloc[-1]) and \
                     (ema10.iloc[-1] > ma20.iloc[-1]) and \
                     (ma20.iloc[-1] > sma50.iloc[-1])
        trend_status = "✅多头" if is_aligned else "❌破位"

        # --- C. RS 强度评分 ---
        spy_aligned = spy_df.reindex(close.index).ffill()
        def get_perf(d): 
            if len(close) < d: return 0.0
            return float((curr_price - close.iloc[-d]) / close.iloc[-d])
        
        # 强化了近3个月(63d)的动能权重
        rs_score = float(get_perf(63)*4 + get_perf(126)*2 + get_perf(250))
        
        # --- D. 紧致度 (弱化处理) ---
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # --- E. 信号捕捉 ---
        signals = []
        base_res = 0
        if is_aligned: signals.append("🌊主升"); base_res += 3
        if rs_score > 1.5: signals.append("🚀超强RS"); base_res += 2
        if curr_price >= float(high.tail(20).max()) * 0.98: signals.append("🎯临界"); base_res += 1

        return {
            "RS_Score": rs_score, 
            "Signals": signals, 
            "Base_Res": base_res, 
            "Price": curr_price, 
            "Tightness": tightness, 
            "ADR": adr,
            "Trend": trend_status
        }
    except Exception as e:
        print(f"计算出错: {e}")
        return None

# ==========================================
# 4. 主指挥引擎
# ==========================================
def run_v1000_reactor():
    start_time = time.time()
    print("🔥 V1000 [10.0 Alpha] 反应堆启动...")

    try:
        # 增加数据获取量以支持均线计算
        data = yf.download(CORE_TICKERS, period="2y", group_by='ticker', threads=True, progress=False)
        spy_df = yf.download("SPY", period="2y", progress=False)['Close'].dropna()
        vix_raw = yf.download("^VIX", period="5d", progress=False)['Close']
        vix = float(vix_raw.iloc[-1]) if not vix_raw.empty else 20.0
    except Exception as e:
        print(f"❌ 数据下载失败: {e}"); return

    candidates = []
    sector_cluster = {}
    
    for t in CORE_TICKERS:
        try:
            df_t = data[t].dropna()
            if df_t.empty: continue
            
            res = calculate_v1000_reactor(df_t, spy_df)
            if res: # 如果 ADR < 3.5%，这里会直接跳过
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP.get(t, "其他")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 过滤器拦截：当前无 ADR > 3.5% 的标的"); return

    # 排序：优先看多头排列和 RS 强度
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(15)
    final_list = []
    
    for i, row in sorted_df.reset_index().iterrows():
        # 评级逻辑：必须是多头排列且RS高
        rating = "💎SSS 共振" if row['Trend'] == "✅多头" and row['RS_Score'] > 2.0 else "🔥活跃"
        sig_str = " + ".join(row['Signals']) if row['Signals'] else "📈 动能观察"
        cluster = f"{sector_cluster.get(row['Sector'], 1)}只活跃"
        
        final_list.append([
            str(row['Ticker']),
            rating,
            sig_str,
            cluster,
            row['Trend'], # 新增：趋势排列状态
            round(float(row['Price']), 2),
            f"{round(float(row['ADR']), 2)}%", # 将 ADR 移到显眼位置
            round(float(row['RS_Score']), 2),
            f"{round(float(row['Tightness']), 2)}%",
            str(row['Sector'])
        ])

    # 构造表头 (10.0版)
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🌋 V1000 Alpha 反应堆 (10.0)", "Update:", bj_now, "VIX:", round(vix, 2), "ADR过滤: >3.5%", "", "", "", ""],
        ["代码", "评级", "核心信号", "板块集群", "趋势对齐", "现价", "ADR(爆点)", "RS强度", "紧致度", "板块"]
    ]
    
    matrix = header + final_list
    
    try:
        payload = json.loads(json.dumps(matrix, default=lambda x: str(x)))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 反应堆同步成功！发现 {len(final_list)} 只高动能标的，耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v1000_reactor()
