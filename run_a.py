import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 熔断配置中心
# ==========================================
# 请确保这里填入的是你真实的 Google 部署 URL
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyaG1UpjC3NLqrqC5T3oIcGM8mnstV-AzlmEDTMdrcfgsOzjzek3aeAqYtg-74ZHv8_/exec"

# 核心观察池
CORE_TICKERS_RAW = [
    "600519", "300750", "601138", "300502", "603501", "688041", "002371", "300308",
    "002475", "002594", "601899", "600030", "600900", "600150", "300274", "000333",
    "688981", "300763", "002415", "603259", "601318", "000651", "600585", "000725"
]

def format_ticker(code):
    if code.startswith('6'): return f"{code}.SS"
    if code.startswith('0') or code.startswith('3'): return f"{code}.SZ"
    if code.startswith('688'): return f"{code}.SS"
    return f"{code}.BJ" if code.startswith('8') or code.startswith('4') else code

# ==========================================
# 2. 安全计算插件 (防止数据溢出)
# ==========================================
def safe_div(n, d):
    """安全除法：防止分母为0或数据溢出"""
    try:
        res = float(n) / (float(d) + 1e-9)
        return res if math.isfinite(res) else 0.0
    except: return 0.0

def clamp(val, min_v, max_v):
    """极值抑制：防止评分爆表"""
    return max(min(val, max_v), min_v)

# ==========================================
# 3. 核心安全引擎
# ==========================================
def calculate_ultimate_safe(df, bench_series, mkt_regime):
    try:
        # A. 基础清洗：剔除无效行并强制转为 float
        df = df.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
        if len(df) < 120: return None
        
        c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
        curr_price = float(c.iloc[-1])
        
        # B. 趋势阶段 (Stage 2)
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        is_stage2 = curr_price > ma200 and ma50 > ma200
        
        # C. 波动率止损 (ATR) - 限制 ATR 异常
        tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(20).mean().iloc[-1]
        # 止损价保护：不能低于现价的 50%，不能高于现价
        stop_loss = clamp(curr_price - (atr * 1.5), curr_price * 0.5, curr_price * 0.99)
        
        # D. 成交量强度 (加装安全阀：最高限制在 10 倍)
        up_v = v[c > c.shift(1)].tail(10).mean()
        dn_v = v[c < c.shift(1)].tail(10).mean()
        vol_ratio = clamp(safe_div(up_v, dn_v), 0.1, 10.0)
        
        # E. 枢轴买点 (20日高点)
        pivot_20d = float(h.tail(20).iloc[:-1].max())
        
        # F. 相对强度评分 (RS) - 归一化处理
        bench_aligned = bench_series.reindex(c.index).ffill()
        rs_line = safe_div(c, bench_aligned)
        
        # 性能评分：限制在 0-100 之间
        perf_score = safe_div(curr_price, c.iloc[-21]) * 20 + safe_div(curr_price, c.iloc[-63]) * 10
        final_score = clamp(perf_score + (vol_ratio * 5), 0, 100)
        
        if not is_stage2: final_score *= 0.6
        if mkt_regime == "Bear": final_score *= 0.8

        action = "观察"
        if is_stage2 and curr_price >= pivot_20d * 0.98 and vol_ratio > 1.2:
            action = "🚀 枢轴突破"
        elif vol_ratio > 1.5 and curr_price > ma50:
            action = "🛡️ 机构吸筹"

        return {
            "score": round(final_score, 2), "action": action, 
            "pivot": round(pivot_20d, 2), "stop": round(stop_loss, 2), 
            "vol": round(vol_ratio, 2), "stage2": "✅" if is_stage2 else "❌"
        }
    except: return None

# ==========================================
# 4. 主流程
# ==========================================
def run_v50_safe_guard():
    start_time = time.time()
    print("🚀 安全版 V50-Guardian 启动，正在拦截溢出数据...")

    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
        m_idx = yf.download("000300.SS", period="1y", progress=False)
        bench = m_idx['Close'].replace([np.inf, -np.inf], np.nan).dropna().squeeze()
        mkt_regime = "Bull" if bench.iloc[-1] > bench.rolling(50).mean().iloc[-1] else "Bear"
    except: return print("数据源中断")

    final_matrix = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            # 兼容 yfinance 多股下载的 Index 结构
            df_t = data[t_full] if isinstance(data.columns, pd.MultiIndex) else data
            res = calculate_ultimate_safe(df_t, bench, mkt_regime)
            
            if res:
                final_matrix.append([
                    t_raw, res['action'], res['stage2'], 
                    res['pivot'], res['stop'], res['vol'], 
                    res['score'], "正常" if res['score'] < 80 else "高度关注", 
                    "A-Share", datetime.datetime.now().strftime('%H:%M')
                ])
        except: continue

    # 排序
    final_matrix.sort(key=lambda x: x[6], reverse=True)
    
    header = [
        ["🏰 V50-SafeGuardian (修正版)", "大盘:", mkt_regime, "安全等级:", "高", "", "", "", "", ""],
        ["代码", "建议指令", "二阶段", "枢轴买点", "科学止损价", "量能比", "综合评分", "风险状态", "市场", "更新时间"]
    ]

    try:
        payload = json.loads(json.dumps(header + final_matrix, default=lambda x: str(x)))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 数据已安全清洗并同步！响应: {resp.text}")
    except Exception as e:
        print(f"同步失败: {e}")

if __name__ == "__main__":
    run_v50_safe_guard()
