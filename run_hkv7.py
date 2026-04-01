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
# 1. 配置中心 (已更新您的最新 URL)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwfstK4Xq1DXft4U3_Qg9pjCQ5Qp0FiIskzrKnT1VFdRiH5FFyk6Iikv0FAcZNrPtp-/exec"

# 港股领袖票池 (涵盖科技、金融、能源、消费龙头)
CORE_TICKERS_HK = [
    "0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK", 
    "0941.HK", "2318.HK", "0005.HK", "9999.HK", "0883.HK",
    "1024.HK", "1299.HK", "2015.HK", "9618.HK", "0939.HK",
    "1398.HK", "2331.HK", "2020.HK", "1177.HK", "2269.HK"
]

# 行业映射
SECTOR_MAP = {
    "0700.HK": "社交/游戏", "3690.HK": "外卖/科技", "9988.HK": "电商/云",
    "1211.HK": "新能源车", "1810.HK": "消费电子", "0941.HK": "电信/红利",
    "2318.HK": "保险/金融", "0005.HK": "银行/汇控", "0883.HK": "石油/中海油",
    "1024.HK": "短视频/快手", "1299.HK": "保险/友邦"
}

# ==========================================
# 2. 核心数据净化器 (防止 JSON 报错)
# ==========================================
def clean_val(v):
    if v is None: return ""
    if isinstance(v, (float, np.float64, np.float32)):
        return round(float(v), 2) if math.isfinite(v) else ""
    return str(v)

# ==========================================
# 3. V1000 领袖演算法
# ==========================================
def calculate_hk_commander(df, bench_df):
    try:
        if len(df) < 150: return None
        
        close = df['Close']
        high = df['High']
        low = df['Low']
        cp = float(close.iloc[-1])
        
        # A. 趋势状态 (Stage 2 判断)
        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1]
        is_bull = cp > ma50 and ma50 > ma200
        
        # B. VCP 紧致度 (Minervini 核心：10天收盘价收缩)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # C. RS 相对强度 (对比恒生指数)
        # 对齐日期计算 60 日涨幅对比
        stock_perf = cp / close.iloc[-60]
        bench_perf = bench_df.iloc[-1] / bench_df.iloc[-60]
        rs_score = stock_perf - bench_perf
        
        # D. 信号决策
        signals = []
        weight = 0
        
        # 信号 1: 奇点觉醒 (RS走强且股价极度收缩，0700爆发常见形态)
        if tightness < 1.6:
            signals.append("👁️奇點觉醒")
            weight += 4
        # 信号 2: 巅峰突破 (接近 52 周高点)
        if cp >= float(high.tail(252).max()) * 0.98:
            signals.append("🚀巔峰突破")
            weight += 2
        
        # E. ADR 波动率
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "Rating": "💎SSS 统帅" if (is_bull and weight >= 4) else ("🔥强势" if is_bull else "观察"),
            "Action": " + ".join(signals) if signals else "趋势保持",
            "Price": cp,
            "Tightness": tightness,
            "RS_Score": rs_score,
            "MA_Status": "多头排布" if is_bull else "均线之下",
            "ADR": adr,
            "Weight": weight
        }
    except:
        return None

# ==========================================
# 4. 执行与同步引擎
# ==========================================
def run_hk_commander():
    start_t = time.time()
    bj_now = (datetime.datetime.now() + datetime.timedelta(hours=0)).strftime('%Y-%m-%d %H:%M')
    print(f"🚀 [{datetime.datetime.now().strftime('%H:%M:%S')}] 开始港股领袖审计...")

    try:
        # 下载数据 (基准: 恒生指数)
        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', threads=True, progress=False)
        bench_df = yf.download("^HSI", period="2y", progress=False)['Close'].dropna()
        hsi_vol = bench_df.pct_change().tail(20).std() * math.sqrt(252) * 100
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    # 构造写入矩阵
    matrix = [
        ["🏰 V1000 统帅版", "同步状态:", "✅ 连通", "更新时间(BJ):", bj_now, "", "", "", "", ""],
        ["代码", "评级", "核心信号", "现价", "紧致度(VCP)", "RS相对强度", "50MA趋势", "ADR(20d)", "板块", "备注"]
    ]

    candidates = []
    for t in CORE_TICKERS_HK:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            res = calculate_hk_commander(df, bench_df)
            
            if res:
                res["Ticker"] = t.replace(".HK", "")
                res["Sector"] = SECTOR_MAP.get(t, "核心蓝筹")
                candidates.append(res)
        except: continue

    # 按权重和强度排序
    sorted_res = sorted(candidates, key=lambda x: (x['Weight'], x['RS_Score']), reverse=True)

    for item in sorted_res:
        matrix.append([
            item["Ticker"],
            item["Rating"],
            item["Action"],
            item["Price"],
            f"{round(item['Tightness'], 2)}%",
            round(item['RS_Score'], 3),
            item["MA_Status"],
            f"{round(item['ADR'], 2)}%",
            item["Sector"],
            "关注" if item['RS_Score'] > 0 else "-"
        ])

    # 发送请求
    try:
        clean_matrix = [[clean_val(c) for c in r] for r in matrix]
        resp = requests.post(WEBAPP_URL, json=clean_matrix, timeout=25)
        
        print(f"DEBUG: Google服务器原始返回 -> {resp.text}")
        if "Success" in resp.text:
            print(f"🎉 港股同步成功！共审计 {len(sorted_res)} 只标的 | 耗时: {round(time.time()-start_t, 2)}s")
        else:
            print(f"⚠️ 服务器响应异常: {resp.text}")
    except Exception as e:
        print(f"❌ 网络同步失败: {e}")

if __name__ == "__main__":
    run_hk_commander()
