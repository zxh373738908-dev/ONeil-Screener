import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import math
import warnings

# 忽略不必要的警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (已更新您的新 URL)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycby3DAv_1VHW1dnSd0Zl2mtMJt1cT7zn417FJ9YPKOxZYokuvTiq40Eby8xs2pljU2yl/exec"

# 港股领袖票池 (覆盖科技、蓝筹、红利)
CORE_TICKERS_HK = [
    "0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK", 
    "9888.HK", "1024.HK", "9618.HK", "0941.HK", "2318.HK",
    "0388.HK", "0005.HK", "2015.HK", "2269.HK", "1177.HK", 
    "2331.HK", "2020.HK", "9999.HK", "6618.HK", "9626.HK",
    "0857.HK", "0883.HK", "1398.HK", "0939.HK", "1299.HK"
]

SECTOR_MAP = {
    "0700.HK": "互联网/社交", "3690.HK": "生活服务", "9988.HK": "电商/云",
    "1211.HK": "新能源车", "1810.HK": "消费电子", "0941.HK": "电信/红利",
    "2318.HK": "金融/保险", "0005.HK": "金融/银行", "9999.HK": "游戏/网易",
    "0883.HK": "能源/石油", "0857.HK": "能源/石油", "1299.HK": "保险/友邦"
}

# ==========================================
# 2. 核心数据净化器 (关键：防止 NaN 破坏传输)
# ==========================================
def clean_val(v):
    if v is None: return ""
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v): return ""
        return round(v, 2)
    if isinstance(v, (np.integer, np.floating)):
        val = float(v)
        return round(val, 2) if math.isfinite(val) else ""
    return str(v)

# ==========================================
# 3. V1000 统帅演算法
# ==========================================
def calculate_hk_commander(df, bench_df):
    try:
        if len(df) < 100: return None
        close, high, low = df['Close'], df['High'], df['Low']
        cp = float(close.iloc[-1])
        
        # A. 相对强度 (对比恒生指数 ^HSI)
        bench_aligned = bench_df.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(20).max()) # RS线20日新高
        
        # B. VCP 紧致度 (收缩判定)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # C. 动能评分 (加权计算)
        def get_ret(d):
            if len(close) < d: return 0.0
            prev = close.iloc[-d]
            return (cp - prev) / prev if prev != 0 else 0.0
        # 权重：近3个月(2.0) + 近半年(1.0)
        rs_score = (get_ret(63) * 2) + get_ret(126)

        # D. 信号决策
        signals, score_weight = [], 0
        # 奇点觉醒：RS走强且股价窄幅横盘 (0700启动前典型特征)
        if rs_nh and tightness < 2.5:
            signals.append("👁️奇點觉醒")
            score_weight += 4
        # 巅峰突破：接近半年内最高价
        if cp >= float(high.tail(126).max()) * 0.975:
            signals.append("🚀巔峰突破")
            score_weight += 2
            
        # E. 趋势过滤
        ma50 = close.rolling(50).mean().iloc[-1]
        is_bull = cp > ma50 # 必须在50日均线之上
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": score_weight, 
            "Price": cp, "Tightness": tightness, "ADR": adr, "RS_NH": rs_nh, "is_bull": is_bull
        }
    except: return None

# ==========================================
# 4. 执行引擎
# ==========================================
def run_hk_commander():
    start_t = time.time()
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    print(f"[{bj_now}] 🛰️ V1000 港股统帅启动审计...")

    try:
        # 下载数据 (基准使用恒指)
        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', threads=True, progress=False)
        bench_df = yf.download("^HSI", period="2y", progress=False)['Close'].dropna()
        # 恒指20日波幅
        hsi_vol = bench_df.pct_change().tail(20).std() * math.sqrt(252) * 100
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    candidates = []
    sector_counts = {}
    
    for t in CORE_TICKERS_HK:
        try:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if df_t.empty or len(df_t) < 60: continue
            
            res = calculate_hk_commander(df_t, bench_df)
            # 过滤：必须多头趋势
            if res and res["is_bull"]:
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP.get(t, "其他板块")
                candidates.append(res)
                sector_counts[res["Sector"]] = sector_counts.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 今日暂无符合逻辑的港股领袖"); return

    # 排序：战法优先级 > 评分
    sorted_res = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(15)
    
    # 构造输出矩阵
    matrix = [
        ["🏰 V1000 港股巅峰统帅", "更新时间:", bj_now, "恒指波动:", f"{round(hsi_vol, 2)}%", "", "", "", "", ""],
        ["代码", "评级", "核心信号", "板块联动", "RS新高", "现价", "紧致度", "强度评分", "ADR波幅", "所属行业"]
    ]
    
    for _, row in sorted_res.iterrows():
        matrix.append([
            row['Ticker'].replace(".HK", ""),
            "💎SSS 统帅" if row['Base_Res'] >= 4 else "🔥强势股",
            " + ".join(row['Signals']) if row['Signals'] else "📈 趋势保持",
            f"{sector_counts.get(row['Sector'], 1)}只活跃",
            "★" if row['RS_NH'] else "-",
            row['Price'],
            f"{round(row['Tightness'], 2)}%",
            row['RS_Score'],
            f"{round(row['ADR'], 2)}%",
            row['Sector']
        ])

    # 执行同步
    try:
        clean_matrix = [[clean_val(c) for c in r] for r in matrix]
        resp = requests.post(WEBAPP_URL, json=clean_matrix, timeout=20)
        
        print(f"DEBUG: 服务器反馈 -> {resp.text}")
        if resp.text == "Success":
            print(f"🎉 港股同步完成！捕捉: {len(sorted_res)} 只标的 | 耗时: {round(time.time() - start_t, 2)}s")
        else:
            print(f"⚠️ 同步失败，原因: {resp.text}")
    except Exception as e:
        print(f"❌ 网络同步异常: {e}")

if __name__ == "__main__":
    run_hk_commander()
