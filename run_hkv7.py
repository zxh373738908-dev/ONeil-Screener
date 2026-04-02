import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请确保 URL 正确)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwfstK4Xq1DXft4U3_Qg9pjCQ5Qp0FiIskzrKnT1VFdRiH5FFyk6Iikv0FAcZNrPtp-/exec"

TOTAL_CAPITAL = 1000000 
MAX_RISK_PER_STOCK = 0.008 # 调低风险系数至 0.8%

CORE_TICKERS_HK = [
    "0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK", 
    "0941.HK", "2318.HK", "0005.HK", "9999.HK", "0883.HK",
    "1024.HK", "1299.HK", "2015.HK", "9618.HK", "0939.HK",
    "1398.HK", "2331.HK", "2020.HK", "1177.HK", "2269.HK", "0388.HK"
]

SECTOR_MAP = {
    "0700.HK": "互联网/游戏", "3690.HK": "生活服务", "9988.HK": "电商/云",
    "1211.HK": "新能源车", "1810.HK": "消费电子", "0941.HK": "电信/红利",
    "2318.HK": "保险/金融", "0005.HK": "银行/公用", "0883.HK": "资源/石油",
    "1024.HK": "短视频/快手", "1299.HK": "保险/友邦", "0388.HK": "交易所"
}

# ==========================================
# 2. 核心算法 (增强自适应性)
# ==========================================
def calculate_quantum_commander(df, hsi_series):
    try:
        if len(df) < 150: return None
        close = df['Close'].ffill()
        high = df['High'].ffill()
        low = df['Low'].ffill()
        vol = df['Volume'].ffill()
        cp = float(close.iloc[-1])
        
        # A. 趋势状态
        ma10 = float(close.rolling(10).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        ma200 = float(close.rolling(200).mean().iloc[-1])
        # 宽松版牛市判定：现价在MA50上方且MA50向上
        is_bull = cp > ma50 and ma50 > close.iloc[-10]
        
        # B. RS 相对强度 (对比恒指)
        bench_aligned = hsi_series.reindex(close.index).ffill()
        rs_line = close / bench_aligned
        rs_nh = bool(float(rs_line.iloc[-1]) >= float(rs_line.tail(30).max())) # 近1.5个月最强
        
        # C. 量能：成交量异动 (RVOL)
        avg_vol_20 = vol.rolling(20).mean().iloc[-1]
        vol_ratio = float(vol.iloc[-1] / avg_vol_20) if avg_vol_20 > 0 else 0
        
        # D. 结构：VCP 紧致度 (放宽到 2.0%)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # E. 风险与止损 (ADR)
        adr = float(((high - low) / low).tail(20).mean() * 100)
        stop_price = cp * (1 - (adr * 0.01 * 1.5))
        
        # F. 信号决策
        signals = []
        score = 0
        if tightness < 1.6: signals.append("👁️奇點觉醒"); score += 4
        if rs_nh: signals.append("🌟RS走强"); score += 3
        if vol_ratio > 1.4: signals.append("🔥量能放量"); score += 2
        if cp > float(high.tail(20).max()) * 0.99: signals.append("🚀突破边缘"); score += 3
        
        # G. 建议头寸
        risk_amt = cp - stop_price
        shares = (TOTAL_CAPITAL * MAX_RISK_PER_STOCK) // risk_amt if risk_amt > 0 else 0

        # --- 判定评级 ---
        if is_bull and score >= 6: rating = "💎SSS 统帅"
        elif is_bull and score >= 3: rating = "🔥强势股"
        elif score >= 4: rating = "✅奇点监控"
        else: rating = "观察"

        return {
            "Rating": rating,
            "Action": " + ".join(signals) if signals else "横盘蓄势",
            "Price": cp,
            "Tightness": tightness,
            "RS_Score": score,
            "Vol_Ratio": vol_ratio,
            "Shares": int(shares),
            "Stop": stop_price,
            "ADR": adr,
            "is_bull": is_bull,
            "Sector": "" # 占位
        }
    except: return None

# ==========================================
# 3. 执行引擎
# ==========================================
def run_hk_commander():
    start_t = time.time()
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    print(f"🚀 [{bj_now}] 启动适应性量子审计...")

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
                res["Sector"] = SECTOR_MAP.get(t, "核心")
                candidates.append(res)
                sector_heat[res["Sector"]] = sector_heat.get(res["Sector"], 0) + 1
        except: continue

    # 排序：评分最高排前面
    candidates.sort(key=lambda x: (x['RS_Score'], x['is_bull']), reverse=True)
    
    # 构造矩阵
    matrix = [
        ["🏰 V1000 适应性版", "环境:", "✅ 审计完成", "时间(BJ):", bj_now, f"恒指波动: {round(hsi_vol,1)}%", "", "", "", ""],
        ["代码", "评级", "核心信号", "建议股数", "现价", "止损价", "紧致度", "RS得分", "量能比", "共振行业"]
    ]

    if not candidates:
        matrix.append(["📭", "当前环境极差", "无符合任何动量信号标的", "-", "-", "-", "-", "-", "-", "-"])
    else:
        for item in candidates[:15]:
            heat_str = f"{item['Sector']}({sector_heat.get(item['Sector'], 1)}只)"
            matrix.append([
                item["Ticker"], item["Rating"], item["Action"], item["Shares"], item["Price"],
                round(item["Stop"], 2), f"{round(item['Tightness'], 2)}%", item["RS_Score"],
                f"{round(item['Vol_Ratio'], 2)}x", heat_str
            ])

    # 同步 Google Sheets
    try:
        def clean_final(v):
            if isinstance(v, (float, int)): return round(v, 2) if math.isfinite(v) else ""
            return str(v)
        
        final_matrix = [[clean_final(c) for c in r] for r in matrix]
        resp = requests.post(WEBAPP_URL, json=final_matrix, timeout=25)
        print(f"🎉 审计同步成功！捕捉标的: {len(candidates)} 只")
    except Exception as e:
        print(f"❌ 网络同步失败: {e}")

if __name__ == "__main__":
    run_hk_commander()
