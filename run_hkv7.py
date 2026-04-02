import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请填入您的 Web App URL)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwfstK4Xq1DXft4U3_Qg9pjCQ5Qp0FiIskzrKnT1VFdRiH5FFyk6Iikv0FAcZNrPtp-/exec"

TOTAL_CAPITAL = 1000000  # 100万港币基准
MAX_RISK_PER_STOCK = 0.008 # 单笔损失控制在总资产 0.8%

CORE_TICKERS_HK = [
    "0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK", 
    "0941.HK", "2318.HK", "0005.HK", "9999.HK", "0883.HK",
    "1024.HK", "1299.HK", "2015.HK", "9618.HK", "0939.HK",
    "1398.HK", "2331.HK", "2020.HK", "1177.HK", "2269.HK", "0388.HK"
]

SECTOR_MAP = {
    "0700.HK": "互联网/社交", "3690.HK": "生活服务", "9988.HK": "电商/云",
    "1211.HK": "新能源车", "1810.HK": "消费电子", "0941.HK": "电信/红利",
    "2318.HK": "保险/金融", "0005.HK": "银行/公用", "0883.HK": "资源/石油",
    "1024.HK": "短视频/快手", "1299.HK": "保险/友邦", "0388.HK": "交易所"
}

# ==========================================
# 🧠 2. 量子统帅核心演算法 (四剑合一版)
# ==========================================
def calculate_quantum_commander(df, hsi_series):
    try:
        if len(df) < 150: return None
        close = df['Close'].ffill()
        high = df['High'].ffill()
        low = df['Low'].ffill()
        vol = df['Volume'].ffill()
        cp = float(close.iloc[-1])
        
        # A. 趋势状态 (适应性 Stage 2)
        ma50 = float(close.rolling(50).mean().iloc[-1])
        ma200 = float(close.rolling(200).mean().iloc[-1])
        is_bull = cp > ma50 # 只要站在50日线上即视为短期转强
        
        # B. RS 觉醒与斜率 (对比恒指)
        bench_aligned = hsi_series.reindex(close.index).ffill()
        rs_line = close / bench_aligned
        rs_ma20 = rs_line.rolling(20).mean()
        # 觉醒：RS线上穿其20日均线
        rs_awakening = rs_line.iloc[-1] > rs_ma20.iloc[-1] and rs_line.iloc[-2] <= rs_ma20.iloc[-2]
        rs_nh = bool(float(rs_line.iloc[-1]) >= float(rs_line.tail(30).max()))
        
        # C. 量能：口袋枢轴 (Pocket Pivot)
        # 当日阳线且成交量 > 过去10天内最大阴线成交量
        neg_days = vol[-11:-1][close[-11:-1] < close[-12:-2]]
        max_neg_vol = float(neg_days.max()) if len(neg_days) > 0 else 9e15
        is_pocket = (close.iloc[-1] > close.iloc[-2]) and (vol.iloc[-1] > max_neg_vol)
        vol_ratio = float(vol.iloc[-1] / vol.rolling(20).mean().iloc[-1])
        
        # D. 结构：VCP 紧致度 (10天收缩)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # E. 风险止损 (动态 ADR)
        adr = float(((high - low) / low).tail(20).mean() * 100)
        # 止损取MA50下方1.5%或ADR回撤
        stop_price = max(ma50 * 0.985, cp * (1 - (adr * 0.01 * 1.5)))
        
        # F. 信号决策
        signals = []
        score = 0
        if is_pocket: signals.append("🎯口袋枢轴"); score += 4
        if rs_awakening: signals.append("🔔强度觉醒"); score += 3
        if rs_nh: signals.append("🌟RS领先"); score += 2
        if tightness < 1.5: signals.append("👁️极致紧致"); score += 4
        if cp >= float(high.tail(252).max()) * 0.98: signals.append("🚀巅峰突破"); score += 3

        # G. 建议头寸 (0.8% 风险模型)
        risk_per_share = cp - stop_price
        shares = (TOTAL_CAPITAL * MAX_RISK_PER_STOCK) // risk_per_share if risk_per_share > 0 else 0

        # --- 判定评级 ---
        if is_bull and score >= 7: rating = "💎SSS 统帅"
        elif is_bull and score >= 4: rating = "🔥强势股"
        elif score >= 5: rating = "✅信号监控"
        else: rating = "观察"

        return {
            "Rating": rating,
            "Action": " + ".join(signals) if signals else "横盘积累",
            "Price": cp,
            "Tightness": tightness,
            "Score": score,
            "Vol_Ratio": vol_ratio,
            "Shares": int(shares),
            "Stop": stop_price,
            "ADR": adr,
            "is_bull": is_bull,
            "RS_NH": rs_nh
        }
    except: return None

# ==========================================
# 3. 执行引擎
# ==========================================
def run_hk_commander():
    start_t = time.time()
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    print(f"🚀 [{bj_now}] 启动 V1000 量子统帅审计...")

    try:
        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', progress=False, threads=False)
        bench_raw = yf.download("^HSI", period="2y", progress=False, threads=False)
        bench_series = bench_raw['Close'].squeeze()
        if isinstance(bench_series, pd.DataFrame): bench_series = bench_series.iloc[:, 0]
        hsi_vol = float(bench_series.pct_change().tail(20).std() * math.sqrt(252) * 100)
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    candidates = []
    sector_heat = {}

    for t in CORE_TICKERS_HK:
        try:
            if t not in data.columns.levels[0]: continue
            res = calculate_quantum_commander(data[t].dropna(), bench_series)
            if res and (res["Rating"] != "观察" or res["is_bull"]):
                res["Ticker"] = t.replace(".HK", "")
                res["Sector"] = SECTOR_MAP.get(t, "核心/其他")
                candidates.append(res)
                sector_heat[res["Sector"]] = sector_heat.get(res["Sector"], 0) + 1
        except: continue

    # 排序：评分优先，RS新高优先
    candidates.sort(key=lambda x: (x['Score'], x['RS_NH']), reverse=True)
    
    # 构造表格矩阵
    matrix = [
        ["🏰 V1000 量子统帅 [合体版]", "状态:", "✅ 审计同步", "北京时间:", bj_now, f"大盘波动: {round(hsi_vol,1)}%", "", "", "", ""],
        ["代码", "统帅评级", "核心信号", "建议股数", "现价", "止损价", "紧致度", "综合评分", "量能比", "行业联动"]
    ]

    if not candidates:
        matrix.append(["📭", "当前环境极差", "未探测到量子共振信号", "-", "-", "-", "-", "-", "-", "-"])
    else:
        for item in candidates[:15]:
            heat_str = f"{item['Sector']}({sector_heat.get(item['Sector'], 1)}只)"
            matrix.append([
                item["Ticker"], item["Rating"], item["Action"], item["Shares"], item["Price"],
                round(item["Stop"], 2), f"{round(item['Tightness'], 2)}%", item["Score"],
                f"{round(item['Vol_Ratio'], 2)}x", heat_str
            ])

    # 同步 Google Sheets
    try:
        def clean_final(v):
            if isinstance(v, (float, int)): return round(v, 2) if math.isfinite(v) else ""
            return str(v)
        
        final_matrix = [[clean_final(c) for c in r] for r in matrix]
        resp = requests.post(WEBAPP_URL, json=final_matrix, timeout=25)
        print(f"🎉 统帅看板更新成功！捕捉: {len(candidates)} 只 | 耗时: {round(time.time()-start_t, 2)}s")
    except Exception as e:
        print(f"❌ 网络同步失败: {e}")

if __name__ == "__main__":
    run_hk_commander()
