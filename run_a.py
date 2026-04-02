import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyaG1UpjC3NLqrqC5T3oIcGM8mnstV-AzlmEDTMdrcfgsOzjzek3aeAqYtg-74ZHv8_/exec"

# 扩充核心池：增加各赛道弹性龙头，建议扩充至 60+ 只
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
# 2. 核心分析引擎 (Ultimate 版)
# ==========================================
def calculate_ultimate_engine(df, bench_series, mkt_regime):
    try:
        if len(df) < 200: return None
        
        c = df['Close'].squeeze()
        h = df['High'].squeeze()
        l = df['Low'].squeeze()
        v = df['Volume'].squeeze()
        curr_price = float(c.iloc[-1])
        
        # --- A. 趋势阶阶段过滤 (Stage 2 Check) ---
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        # 核心：股价必须在200日线上方，且50日线也在200日线上方（典型的多头趋势）
        is_stage2 = curr_price > ma200 and ma50 > ma200
        
        # --- B. 波动率与科学止损 (ATR Stop Loss) ---
        # 计算 20 日 ATR (平均真实波幅)
        tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(20).mean().iloc[-1]
        stop_loss = curr_price - (atr * 1.5) # 1.5倍ATR止损
        
        # --- C. 枢轴买点与成交量强度 ---
        pivot_20d = float(h.tail(20).iloc[:-1].max())
        up_vol = v[c > c.shift(1)].tail(10).mean()
        dn_vol = v[c < c.shift(1)].tail(10).mean()
        vol_ratio = up_vol / (dn_vol + 1)
        
        # --- D. 相对强度评分 ---
        bench_aligned = bench_series.reindex(c.index).ffill()
        rs_line = (c / bench_aligned).dropna()
        rs_nh = rs_line.iloc[-1] >= rs_line.tail(120).max() # 半年RS新高
        
        # 计算综合评分
        score = (curr_price/c.iloc[-21]*40) + (vol_ratio*20)
        if not is_stage2: score *= 0.5 # 非二阶段趋势，评分减半
        if mkt_regime == "Bear": score *= 0.7

        # 战术标签
        action = "持有/观察"
        if is_stage2 and curr_price >= pivot_20d * 0.98 and vol_ratio > 1.3:
            action = "🚀 枢轴突破"
        elif rs_nh and (c.tail(10).std()/c.tail(10).mean()) < 0.02:
            action = "👁️ 奇点缩量"

        return {
            "score": score, "action": action, "pivot": pivot_20d,
            "stop": stop_loss, "vol": vol_ratio, "stage2": "✅" if is_stage2 else "❌"
        }
    except: return None

# ==========================================
# 3. 主流程
# ==========================================
def run_v50_ultimate():
    start_time = time.time()
    print("🛡️ V50-Ultimate 终极审计启动...")

    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False)
        m_idx = yf.download("000300.SS", period="1y", progress=False)
        bench = m_idx['Close'].squeeze()
        mkt_regime = "Bull" if bench.iloc[-1] > bench.rolling(50).mean().iloc[-1] else "Bear"
    except: return print("数据源异常")

    results = []
    sector_resonance = {}

    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            df_t = data[t_full].dropna()
            res = calculate_ultimate_engine(df_t, bench, mkt_regime)
            if res:
                res["ticker"] = t_raw
                results.append(res)
                # 统计板块异动（简单映射，可扩展）
                # sector_resonance[...] += 1
        except: continue

    # 排序与导出
    sorted_df = sorted(results, key=lambda x: x['score'], reverse=True)[:20]
    
    final_matrix = []
    header = [
        ["🏰 V50-Ultimate 终极枢纽", "大盘:", mkt_regime, "Update:", datetime.datetime.now().strftime('%H:%M'), "", "", "", "", ""],
        ["代码", "状态/建议", "二阶段", "枢轴买点", "科学止损", "量能比", "综合评分", "风险提示", "市场", "RS地位"]
    ]
    
    for r in sorted_df:
        risk_msg = "风险高" if r['score'] < 40 else "趋势稳健"
        final_matrix.append([
            r['ticker'], r['action'], r['stage2'], 
            round(r['pivot'], 2), round(r['stop'], 2),
            round(r['vol'], 2), round(r['score'], 2),
            risk_msg, "A-Share", "Strong" if r['score'] > 60 else "Normal"
        ])

    try:
        payload = json.loads(json.dumps(header + final_matrix, default=lambda x: str(x)))
        requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 终极版同步成功！耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"同步失败: {e}")

if __name__ == "__main__":
    run_v50_ultimate()
