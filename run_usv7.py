import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import warnings
import time
import math
import requests
import json
from polygon import RESTClient

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
# [填入你的 API KEY 和 之前生成的部署 URL]
POLYGON_API_KEY = "您的_POLYGON_API_KEY"
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyVyClySDklAPrx30URWZOGyb423Vb_5Dzt7WCKQ6WJwcNb7HLqwD0ckiMYm5sTwnLz/exec"

client_poly = RESTClient(POLYGON_API_KEY)

# 精选池：核心科技股 + 资源股 + 指数
CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "CF", "PR", "GOOGL", "AAPL", "MSFT", 
    "META", "AMZN", "AMD", "COIN", "MARA", "SMCI", "AVGO", "LLY", "VRT"
]

# ==========================================
# 2. 净化引擎 (防止 JSON 报错)
# ==========================================
def hidden_dragon_clean(val):
    if val is None or (isinstance(val, float) and not math.isfinite(val)): return ""
    if isinstance(val, (np.floating, float)): return float(round(val, 3))
    if isinstance(val, (np.integer, int)): return int(val)
    return str(val)

# ==========================================
# 3. 核心算法：V1000 枢纽共振逻辑
# ==========================================
def calculate_v1000_nexus(df, spy_df):
    try:
        if len(df) < 150: return None
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = close.iloc[-1]
        
        # 均线与成交量
        ma50 = close.rolling(50).mean().iloc[-1]
        vol_ma50 = vol.rolling(50).mean().iloc[-1]

        # 相对强度 (RS) 与 紧致度 (VCP)
        rs_line = close / spy_df
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max()
        tightness = (close.tail(10).std() / close.tail(10).mean()) * 100
        
        # RS 性能评分 (6个月权重最高)
        def get_perf(d): return (curr_price - close.iloc[-d]) / close.iloc[-d]
        rs_score = (get_perf(63)*2 + get_perf(126) + get_perf(189))

        signals, base_res = [], 0
        if rs_nh_20 and tightness < 1.3: signals.append("👁️奇點"); base_res += 3
        if curr_price >= high.tail(252).max() * 0.98 and vol.iloc[-1] > vol_ma50: signals.append("🚀突破"); base_res += 2
        if rs_score > 0.4 and abs(curr_price - ma50)/ma50 < 0.03: signals.append("🐉回頭"); base_res += 2

        if not signals: return None
        adr = ((high - low) / low).tail(20).mean() * 100
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr
        }
    except: return None

# ==========================================
# 4. 极速期权审计 (限额前2名)
# ==========================================
def get_option_audit(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        c_val, p_val = 0, 0
        for s in snaps[:100]: # 只扫描前100个合约增加速度
            v = s.day.volume if s.day else 0
            if v < 50: continue
            val = v * (s.day.last or 0) * 100
            if s.details.contract_type == 'call': c_val += val
            else: p_val += val
        call_pct = round(c_val / (c_val + p_val + 1) * 100, 1)
        return f"{call_pct}%"
    except: return "N/A"

# ==========================================
# 5. 主执行流程
# ==========================================
def run_v1000_nexus_bridge():
    start_time = time.time()
    print("🏟️ V1000 枢纽系统启动 (极致优化版)...")
    
    # 1. 下载基础数据
    print("📥 获取市场基准...")
    env = yf.download(["SPY", "^VIX"], period="1y", progress=False)['Close']
    spy_df = env['SPY'].dropna(); vix = env['^VIX'].iloc[-1]
    
    # 2. 批量扫描
    print(f"🚀 正在演算 {len(CORE_TICKERS)} 只核心标的...")
    data = yf.download(CORE_TICKERS, period="1y", group_by='ticker', threads=True, progress=False)
    
    candidates = []
    for t in CORE_TICKERS:
        try:
            res = calculate_v1000_nexus(data[t].dropna(), spy_df)
            if res:
                res["Ticker"] = t
                candidates.append(res)
        except: continue

    if not candidates:
        print("📭 今日暂无共振信号。")
        return

    # 3. 排序并执行精简期权审计 (仅对前3名，避免 Polygon 封锁)
    final_df = pd.DataFrame(candidates).sort_values(by="Base_Res", ascending=False).head(12)
    results = []
    
    print("🔥 执行期权流穿透 (前3名)...")
    for i, row in final_df.iterrows():
        opt_call = "N/A"
        if i < 3: # 只有前3名触发 Polygon 审计，确保不超时
            opt_call = get_option_audit(row['Ticker'])
            time.sleep(1) # 轻微停顿
        
        results.append([
            row['Ticker'],
            "💎SSS" if row['Base_Res'] >= 5 else "🔥强势",
            " + ".join(row['Signals']),
            opt_call,
            row['Price'],
            f"{round(row['Tightness'],2)}%",
            round(row['RS_Score'], 2),
            f"{round(row['ADR'],2)}%"
        ])

    # 4. 构造矩阵发送 (Apps Script 桥接)
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🏰 V1000 枢纽 (8.0版)", "Update:", bj_now, "VIX:", round(vix, 2)],
        ["代码", "评级", "共振信号", "看涨% (Top3)", "现价", "紧致度", "RS强度", "ADR"]
    ]
    
    matrix = header + [[hidden_dragon_clean(cell) for cell in row] for row in results]

    print("📤 正在通过桥接隧道上传...")
    try:
        resp = requests.post(WEBAPP_URL, data=json.dumps(matrix), timeout=10)
        if resp.text == "Success":
            print(f"🎉 任务达成！耗时: {round(time.time() - start_time, 2)}s")
        else:
            print(f"⚠️ 同步异常: {resp.text}")
    except Exception as e:
        print(f"❌ 桥接失败: {e}")

if __name__ == "__main__":
    run_v1000_nexus_bridge()
