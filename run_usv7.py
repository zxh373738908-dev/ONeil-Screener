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
# 2. 深度净化工具 (防止 JSON 报错)
# ==========================================
def safe_val(v, is_num=True):
    """强制转换任何对象为基础 Python 类型"""
    try:
        if v is None: return 0.0 if is_num else ""
        # 处理 Pandas Series 或 NumPy 类型
        if hasattr(v, 'iloc'): v = v.iloc[0]
        if isinstance(v, (np.floating, np.integer, float, int)):
            return float(v) if math.isfinite(v) else 0.0
        return str(v)
    except:
        return 0.0 if is_num else str(v)

# ==========================================
# 3. 核心算法逻辑
# ==========================================
def calculate_v1000_nexus(df, spy_df):
    try:
        if len(df) < 60: return None
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = float(close.iloc[-1])
        
        # 相对强度 (RS)
        spy_aligned = spy_df.reindex(close.index).ffill()
        rs_line = (close / spy_aligned).dropna()
        rs_nh_20 = bool(rs_line.iloc[-1] >= rs_line.tail(20).max()) if not rs_line.empty else False
        
        # 紧致度 (VCP)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # RS 评分 (加权)
        def get_perf(d): 
            if len(close) < d: return 0.0
            return float((curr_price - close.iloc[-d]) / close.iloc[-d])
        
        rs_score = float(get_perf(63)*3 + get_perf(126)*2 + get_perf(250))

        signals, base_res = [], 0
        if rs_nh_20 and tightness < 2.0: 
            signals.append("👁️奇點"); base_res += 4
        if curr_price >= float(high.tail(100).max()) * 0.95: 
            signals.append("🚀高位"); base_res += 2
        
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr
        }
    except:
        return None

# ==========================================
# 4. 主指挥引擎
# ==========================================
def run_v1000_final():
    start_time = time.time()
    print("🚀 V1000 [9.4加固版] 启动...")

    try:
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
            if df_t.empty or len(df_t) < 20: continue
            
            res = calculate_v1000_nexus(df_t, spy_df)
            if res:
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP.get(t, "其他")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 无候选标的"); return

    # 排序并取前 12 名
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(12)
    final_list = []
    
    print(f"✅ 找到 {len(sorted_df)} 只标的，处理 JSON 序列化...")
    for i, row in sorted_df.reset_index().iterrows():
        # 强制转换为基础 Python 类型
        t_code = str(row['Ticker'])
        rating = "💎SSS 共振" if row['Base_Res'] >= 4 else ("🔥强势" if row['RS_Score'] > 0.5 else "✅监控")
        sig_str = " + ".join(row['Signals']) if row['Signals'] else "📈 趋势保持"
        cluster = f"{sector_cluster.get(row['Sector'], 1)}只活跃"
        
        final_list.append([
            t_code,
            rating,
            sig_str,
            cluster,
            "N/A", # 简化期权审计以防报错
            round(float(row['Price']), 2),
            f"{round(float(row['Tightness']), 2)}%",
            round(float(row['RS_Score']), 2),
            f"{round(float(row['ADR']), 2)}%",
            str(row['Sector'])
        ])

    # 构造表头并强制清理
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🏰 V1000 终极枢纽 (9.4版)", "Update:", bj_now, "VIX:", round(vix, 2), "", "", "", "", ""],
        ["代码", "评级", "枢纽信号", "板块集群", "看涨% (Top2)", "现价", "紧致度", "RS强度", "ADR", "板块"]
    ]
    
    matrix = header + final_list
    
    try:
        # 使用 json.dumps 的一种更安全方式
        payload = json.loads(json.dumps(matrix, default=lambda x: str(x)))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 同步完成！耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v1000_final()
