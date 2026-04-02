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

# 1. 配置中心 (请填入您最新的 Web App URL)

# ==========================================

WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwfstK4Xq1DXft4U3_Qg9pjCQ5Qp0FiIskzrKnT1VFdRiH5FFyk6Iikv0FAcZNrPtp-/exec"

# 港股领袖票池

CORE_TICKERS_HK = [

    "0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK", 

    "0941.HK", "2318.HK", "0005.HK", "9999.HK", "0883.HK",

    "1024.HK", "1299.HK", "2015.HK", "9618.HK", "0939.HK",

    "1398.HK", "2331.HK", "2020.HK", "1177.HK", "2269.HK"

]

SECTOR_MAP = {

    "0700.HK": "社交/游戏", "3690.HK": "外卖/科技", "9988.HK": "电商/云",

    "1211.HK": "新能源车", "1810.HK": "消费电子", "0941.HK": "电信/红利",

    "2318.HK": "保险/金融", "0005.HK": "银行/汇控", "0883.HK": "石油/中海油",

    "1024.HK": "短视频/快手", "1299.HK": "保险/友邦"

}

# ==========================================

# 2. 核心净化工具

# ==========================================

def clean_val(v):

    if v is None: return ""

    try:

        if isinstance(v, (pd.Series, np.ndarray)):

            v = v.iloc[-1] if len(v) > 0 else 0.0

        if hasattr(v, 'item'): 

            v = v.item()

        v = float(v)

        return round(v, 2) if math.isfinite(v) else ""

    except:

        return str(v)

# ==========================================

# 3. V1000 领袖演算法

# ==========================================

def calculate_hk_commander(df, bench_series):

    try:

        if len(df) < 150: return None

        # 提取数据并填补缺失值

        close = df['Close'].ffill()

        high = df['High'].ffill()

        low = df['Low'].ffill()

        cp = float(close.iloc[-1])

        # A. 趋势状态 (Stage 2)

        ma50 = float(close.rolling(50).mean().iloc[-1])

        ma200 = float(close.rolling(200).mean().iloc[-1])

        is_bull = bool(cp > ma50 and ma50 > ma200)

        # B. VCP 紧致度 (10日波动)

        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)

        # C. RS 相对强度 (计算 60 日收益差)

        stock_ret = cp / float(close.iloc[-60])

        bench_ret = float(bench_series.iloc[-1]) / float(bench_series.iloc[-60])

        rs_score = float(stock_ret - bench_ret)

        # 判断 RS 是否创 20 日新高

        # 对齐索引防止日期错位

        bench_aligned = bench_series.reindex(close.index).ffill()

        rs_line = close / bench_aligned

        # 使用 .max() 并确保返回标量

        rs_max_20 = float(rs_line.tail(20).max())

        rs_nh = bool(float(rs_line.iloc[-1]) >= rs_max_20)

        # D. 信号决策

        signals = []

        weight = 0

        if tightness < 1.6:

            signals.append("👁️奇點觉醒")

            weight += 4

        if cp >= float(high.tail(252).max()) * 0.98:

            signals.append("🚀巔峰突破")

            weight += 2

        # E. ADR

        adr = float(((high - low) / low).tail(20).mean() * 100)

        return {

            "Rating": "💎SSS 统帅" if (is_bull and weight >= 4) else ("🔥强势" if is_bull else "观察"),

            "Action": " + ".join(signals) if signals else "趋势保持",

            "Price": cp,

            "Tightness": tightness,

            "RS_Score": rs_score,

            "MA_Status": "多头排布" if is_bull else "均线之下",

            "ADR": adr,

            "Weight": int(weight),

            "RS_NH": rs_nh

        }

    except Exception:

        return None

# ==========================================

# 4. 执行引擎

# ==========================================

def run_hk_commander():

    start_t = time.time()

    # 强制获取北京时间

    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')

    print(f"🚀 [{bj_now}] 开始港股领袖审计...")

    try:

        # threads=False 彻底解决 "database is locked" 错误

        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', progress=False, threads=False)

        bench_raw = yf.download("^HSI", period="2y", progress=False, threads=False)

        # 确保 bench_series 是单一 Series (使用 squeeze)

        bench_series = bench_raw['Close'].squeeze()

        if isinstance(bench_series, pd.DataFrame): # 如果 squeeze 失败

            bench_series = bench_series.iloc[:, 0]

        hsi_vol = float(bench_series.pct_change().tail(20).std()  math.sqrt(252)  100)

    except Exception as e:

        print(f"❌ 数据抓取失败: {e}"); return

    candidates = []

    for t in CORE_TICKERS_HK:

        try:

            if t not in data.columns.levels[0]: continue

            df_t = data[t].dropna()

            if len(df_t) < 100: continue

            res = calculate_hk_commander(df_t, bench_series)

            if res:

                # 过滤条件

                if res["MA_Status"] == "多头排布" or res["Weight"] > 0:

                    res["Ticker"] = t.replace(".HK", "")

                    res["Sector"] = SECTOR_MAP.get(t, "核心蓝筹")

                    candidates.append(res)

        except: continue

    # 排序：确保所有比较对象都是基础 Python 类型

    candidates.sort(key=lambda x: (int(x['Weight']), float(x['RS_Score'])), reverse=True)

    sorted_res = candidates[:15]

    # 构造写入矩阵

    matrix = [

        ["🏰 V1000 统帅版", "状态:", "✅ 连通", "更新(BJ):", bj_now, f"大盘波动: {round(hsi_vol,1)}%", "", "", "", ""],

        ["代码", "评级", "核心信号", "现价", "紧致度(VCP)", "RS相对强度", "50MA趋势", "ADR(20d)", "板块", "RS新高"]

    ]

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

            "★" if item.get("RS_NH") else "-"

        ])

    # 最终净化并发送

    try:

        # 再次强制转换为 JSON 友好格式

        clean_matrix = []

        for row in matrix:

            clean_row = [clean_val(c) if isinstance(c, (float, int, np.float64, np.int64)) else str(c) for c in row]

            clean_matrix.append(clean_row)

        resp = requests.post(WEBAPP_URL, json=clean_matrix, timeout=25)

        print(f"DEBUG: 服务器返回 -> {resp.text}")

        if "Success" in resp.text:

            print(f"🎉 港股同步成功！已捕捉标的写入表格。")

    except Exception as e:

        print(f"❌ 网络同步失败: {e}")

if __name__ == "__main__":

    run_hk_commander()
