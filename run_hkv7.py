import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请填入您的 Web App URL)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwfstK4Xq1DXft4U3_Qg9pjCQ5Qp0FiIskzrKnT1VFdRiH5FFyk6Iikv0FAcZNrPtp-/exec"

TOTAL_CAPITAL = 1000000 
MAX_RISK_PER_STOCK = 0.008 

# 核心监控（必出标的）
LEADER_WATCH = ["0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK"]

CORE_TICKERS_HK = list(set(LEADER_WATCH + [
    "0941.HK", "2318.HK", "0005.HK", "9999.HK", "0883.HK",
    "1024.HK", "1299.HK", "2015.HK", "9618.HK", "0939.HK",
    "1398.HK", "2331.HK", "2020.HK", "1177.HK", "2269.HK", "0388.HK"
]))

# ==========================================
# 🧠 2. 量子哨兵演算法 (修复索引对齐 Bug)
# ==========================================
def calculate_sentinel_metrics(df, hsi_series):
    try:
        if df is None or len(df) < 60: return None
        
        # 强制转换为基础 Series 并填充
        close = df['Close'].ffill()
        high = df['High'].ffill()
        low = df['Low'].ffill()
        vol = df['Volume'].ffill()
        cp = float(close.iloc[-1])
        
        # A. 均线状态
        ma10 = float(close.rolling(10).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        is_bull = cp > ma50
        
        # B. RS 觉醒 (忽略索引对比)
        bench_aligned = hsi_series.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        if len(rs_line) < 20: return None
        
        rs_ma20 = rs_line.rolling(20).mean()
        # 显式使用 iloc 标量对比，避免 Series 对比报错
        rs_awakening = float(rs_line.iloc[-1]) > float(rs_ma20.iloc[-1])
        
        # C. 紧致度 (收缩判定)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # D. 口袋枢轴 (修复关键：使用 .values 忽略日期索引)
        # 获取过去10天的收盘价和成交量切片
        v_slice = vol.iloc[-11:-1].values
        c_slice = close.iloc[-11:-1].values
        c_prev_slice = close.iloc[-12:-2].values
        
        # 找出过去10天里的阴线成交量
        neg_vol_list = v_slice[c_slice < c_prev_slice]
        max_neg_vol = float(np.max(neg_vol_list)) if len(neg_vol_list) > 0 else 9e15
        
        # 当日成交量对比
        is_pocket = (cp > float(close.iloc[-2])) and (float(vol.iloc[-1]) > max_neg_vol)

        # E. 评分系统
        score = 0
        signals = []
        if is_pocket: signals.append("🎯口袋枢轴"); score += 4
        if rs_awakening: signals.append("🔔RS觉醒"); score += 3
        if tightness < 1.8: signals.append("👁️紧致"); score += 3
        if cp > ma10: signals.append("📈转强"); score += 2

        # 状态判定
        if is_bull and score >= 6: rating = "💎SSS 统帅"
        elif is_bull: rating = "🔥多头趋势"
        elif cp > ma10: rating = "✅短线转强"
        else: rating = "❄️均线压制"

        adr = float(((high - low) / low).tail(20).mean() * 100)
        stop_p = max(ma50 * 0.98, cp * (1 - (adr * 0.01 * 1.5)))
        shares = (TOTAL_CAPITAL * MAX_RISK_PER_STOCK) // (cp - stop_p) if cp > stop_p else 0

        return {
            "Rating": rating, "Action": " + ".join(signals) if signals else "震荡蓄势",
            "Price": cp, "Tightness": tightness, "Score": score,
            "Shares": int(shares), "Stop": stop_p, "is_bull": is_bull
        }
    except Exception as e:
        # 打印详细错误到终端以便调试
        print(f"Error detail: {e}")
        return {"Rating": "⚠️计算异常", "Action": str(e)[:15], "Price": 0, "Tightness": 0, "Score": 0, "Shares": 0, "Stop": 0, "is_bull": False}

# ==========================================
# 🚀 3. 执行引擎
# ==========================================
def run_sentinel_commander():
    start_t = time.time()
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    print(f"🚀 [{bj_now}] 启动量子哨兵最终加固版...")

    try:
        # threads=False 避免数据库锁定
        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', progress=False, threads=False)
        bench_raw = yf.download("^HSI", period="2y", progress=False, threads=False)
        bench_series = bench_raw['Close'].squeeze()
        if isinstance(bench_series, pd.DataFrame): bench_series = bench_series.iloc[:, 0]
        hsi_vol = float(bench_series.pct_change().tail(20).std() * math.sqrt(252) * 100)
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    candidates = []
    bull_count = 0
    
    for t in CORE_TICKERS_HK:
        try:
            # 获取个股数据切片
            df_t = data[t].dropna() if t in data.columns.levels[0] else None
            res = calculate_sentinel_metrics(df_t, bench_series)
            
            if res:
                res["Ticker"] = t.replace(".HK", "")
                if res["is_bull"]: bull_count += 1
                
                # 策略：LEADER_WATCH 必出，其他有信号才出
                if (t in LEADER_WATCH) or (res["Score"] > 0) or (res["is_bull"]):
                    candidates.append(res)
        except: continue

    # 排序：Leader 强制置顶，其余按评分排
    def sort_key(x):
        is_leader = 1 if (x['Ticker'] + ".HK") in LEADER_WATCH else 0
        return (is_leader, x['Score'])

    candidates.sort(key=sort_key, reverse=True)
    
    # 构造写入矩阵
    mkt_breadth = f"{round((bull_count / len(CORE_TICKERS_HK)) * 100, 1)}%"
    matrix = [
        ["🏰 V1000 量子哨兵 [最终加固]", "多头广度:", mkt_breadth, "北京时间:", bj_now, f"大盘波动: {round(hsi_vol,1)}%", "", "", "", ""],
        ["代码", "哨兵评级", "核心信号", "建议股数", "现价", "止损参考", "紧致度", "强度评分", "趋势状态", "板块"]
    ]

    for item in candidates[:25]:
        matrix.append([
            item["Ticker"], item["Rating"], item["Action"], item["Shares"], item["Price"],
            round(item["Stop"], 2), f"{round(item['Tightness'], 2)}%", item["Score"],
            "多头" if item["is_bull"] else "空头", "核心蓝筹"
        ])

    # 同步 Google Sheets
    try:
        def clean_final(v):
            if isinstance(v, (float, int)): return round(v, 2) if math.isfinite(v) else ""
            return str(v)
        
        final_matrix = [[clean_final(c) for c in r] for r in matrix]
        resp = requests.post(WEBAPP_URL, json=final_matrix, timeout=25)
        print(f"🎉 审计同步成功！监控列表: {len(candidates)} 只 | 多头广度: {mkt_breadth}")
    except Exception as e:
        print(f"❌ 网络同步失败: {e}")

if __name__ == "__main__":
    run_sentinel_commander()
