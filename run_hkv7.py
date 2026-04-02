import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请替换您的最新 URL)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwfstK4Xq1DXft4U3_Qg9pjCQ5Qp0FiIskzrKnT1VFdRiH5FFyk6Iikv0FAcZNrPtp-/exec"

# 账户设置 (用于计算建议股数)
TOTAL_CAPITAL = 1000000  # 100万港币
MAX_RISK_PER_STOCK = 0.01  # 单笔损失控制在总资产 1%

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
    "2020.HK": "体育用品", "2331.HK": "体育用品", "0388.HK": "交易所"
}

# ==========================================
# 2. 工具函数
# ==========================================
def clean_val(v):
    if v is None: return ""
    try:
        if isinstance(v, (pd.Series, np.ndarray)): v = v.iloc[-1]
        v = float(v)
        return round(v, 2) if math.isfinite(v) else ""
    except: return str(v)

# ==========================================
# 3. V1000 增强演算法
# ==========================================
def calculate_quantum_commander(df, hsi_series):
    try:
        if len(df) < 200: return None
        close = df['Close'].ffill()
        high = df['High'].ffill()
        low = df['Low'].ffill()
        vol = df['Volume'].ffill()
        cp = float(close.iloc[-1])
        
        # A. 趋势模板 (Minervini Stage 2)
        ma50 = float(close.rolling(50).mean().iloc[-1])
        ma150 = float(close.rolling(150).mean().iloc[-1])
        ma200 = float(close.rolling(200).mean().iloc[-1])
        # 股价 > MA50 > MA150 > MA200 且 MA200 至少平稳
        is_stage_2 = cp > ma50 > ma150 > ma200
        
        # B. 动能：RS 相对强度
        bench_aligned = hsi_series.reindex(close.index).ffill()
        rs_line = close / bench_aligned
        rs_nh = bool(float(rs_line.iloc[-1]) >= float(rs_line.tail(20).max()))
        rs_score = float((cp / close.iloc[-63]) - (bench_aligned.iloc[-1] / bench_aligned.iloc[-63]))
        
        # C. 能量：成交量异动 (RVOL)
        avg_vol_20 = vol.rolling(20).mean().iloc[-1]
        vol_ratio = float(vol.iloc[-1] / avg_vol20) if avg_vol20 > 0 else 0
        
        # D. 结构：VCP 紧致度
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # E. 风险：ADR 波幅与止损计算
        adr = float(((high - low) / low).tail(20).mean() * 100)
        stop_loss_price = cp * (1 - (adr * 0.01 * 1.5)) # 1.5倍ADR作为止损空间
        
        # F. 建议头寸 (Position Sizing)
        # 每股风险金额 = 现价 - 止损价
        risk_per_share = cp - stop_loss_price
        suggested_shares = 0
        if risk_per_share > 0:
            # 允许总资产 1% 的风险损失
            suggested_shares = (TOTAL_CAPITAL * MAX_RISK_PER_STOCK) // risk_per_share

        # G. 战法决策
        signals = []
        weight = 0
        if tightness < 1.4: signals.append("👁️奇點觉醒"); weight += 4
        if vol_ratio > 1.5 and cp > close.iloc[-2]: signals.append("🔥量能激增"); weight += 2
        if cp >= float(high.tail(252).max()) * 0.98: signals.append("🚀巔峰突破"); weight += 3
        
        return {
            "Rating": "💎SSS 统帅" if (is_stage_2 and weight >= 5) else ("🔥强势" if is_stage_2 else "观察"),
            "Action": " + ".join(signals) if signals else "趋势保持",
            "Price": cp,
            "Tightness": tightness,
            "RS_Score": rs_score,
            "Vol_Ratio": vol_ratio,
            "Shares": int(suggested_shares),
            "Stop": stop_loss_price,
            "ADR": adr,
            "Weight": int(weight),
            "RS_NH": rs_nh,
            "is_stage_2": is_stage_2
        }
    except: return None

# ==========================================
# 4. 执行引擎
# ==========================================
def run_hk_commander():
    start_t = time.time()
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    print(f"🚀 [{bj_now}] 启动量子统帅审计...")

    try:
        # 下载数据 (threads=False 保证稳定性)
        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', progress=False, threads=False)
        bench_raw = yf.download("^HSI", period="2y", progress=False, threads=False)
        bench_series = bench_raw['Close'].squeeze()
        if isinstance(bench_series, pd.DataFrame): bench_series = bench_series.iloc[:, 0]
        hsi_vol = float(bench_series.pct_change().tail(20).std() * math.sqrt(252) * 100)
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    candidates = []
    sector_heat = {} # 统计板块热度

    for t in CORE_TICKERS_HK:
        try:
            if t not in data.columns.levels[0]: continue
            res = calculate_quantum_commander(data[t].dropna(), bench_series)
            if res and (res["is_stage_2"] or res["Weight"] > 3):
                res["Ticker"] = t.replace(".HK", "")
                res["Sector"] = SECTOR_MAP.get(t, "其他")
                candidates.append(res)
                # 累加板块信号数
                sector_heat[res["Sector"]] = sector_heat.get(res["Sector"], 0) + 1
        except: continue

    # 排序
    candidates.sort(key=lambda x: (int(x['Weight']), float(x['RS_Score'])), reverse=True)
    
    # 构造矩阵
    matrix = [
        ["🏰 V1000 统帅 Pro", "环境:", "✅ 审计完成", "时间(BJ):", bj_now, f"恒指波动: {round(hsi_vol,1)}%", "", "", "", ""],
        ["代码", "评级", "核心信号", "建议股数(手)", "现价", "止损价", "紧致度", "RS强度", "量能比", "行业热度"]
    ]

    for item in candidates[:15]:
        heat_str = f"{item['Sector']} ({sector_heat.get(item['Sector'], 1)}只共振)"
        matrix.append([
            item["Ticker"],
            item["Rating"],
            item["Action"],
            item["Shares"],
            item["Price"],
            round(item["Stop"], 2),
            f"{round(item['Tightness'], 2)}%",
            round(item['RS_Score'], 3),
            f"{round(item['Vol_Ratio'], 2)}x",
            heat_str
        ])

    # 同步 Google Sheets
    try:
        final_matrix = [[str(cell) if not isinstance(cell, (int, float)) else cell for cell in row] for row in matrix]
        final_matrix = [[clean_val(c) if isinstance(c, (float, int)) else c for c in r] for r in final_matrix]
        resp = requests.post(WEBAPP_URL, json=final_matrix, timeout=25)
        if "Success" in resp.text:
            print(f"🎉 审计同步成功！发现 {len(candidates)} 只符合条件标的。")
    except Exception as e:
        print(f"❌ 网络同步失败: {e}")

if __name__ == "__main__":
    run_hk_commander()
