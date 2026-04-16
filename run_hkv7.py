import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwfstK4Xq1DXft4U3_Qg9pjCQ5Qp0FiIskzrKnT1VFdRiH5FFyk6Iikv0FAcZNrPtp-/exec"

TOTAL_CAPITAL = 1000000 
MAX_RISK_PER_STOCK = 0.008 

# 核心监控（必出标的）
LEADER_WATCH =["0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK"]

CORE_TICKERS_HK = list(set(LEADER_WATCH +[
    "0941.HK", "2318.HK", "0005.HK", "9999.HK", "0883.HK",
    "1024.HK", "1299.HK", "2015.HK", "9618.HK", "0939.HK",
    "1398.HK", "2331.HK", "2020.HK", "1177.HK", "2269.HK", "0388.HK"
]))

# ==========================================
# 🧠 2. 量子哨兵演算法 (V1000 蓝筹绝对防御版)
# ==========================================
def calculate_sentinel_metrics(df, hsi_series, rs_rank_series):
    try:
        if df is None or len(df) < 60: return None
        
        # 强制转换为基础 Series 并填充
        close = df['Close'].ffill()
        high = df['High'].ffill()
        low = df['Low'].ffill()
        vol = df['Volume'].ffill()
        cp = float(close.iloc[-1])
        
        # A. 均线与偏离度 (Ext_50)
        ma10 = float(close.rolling(10).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        is_bull = cp > ma50
        ext_50 = ((cp - ma50) / ma50) * 100  # 距离50日线的百分比
        
        # B. 横向与纵向 RS 数据
        bench_aligned = hsi_series.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        if len(rs_line) < 20: return None
        
        rs_ma20 = rs_line.rolling(20).mean()
        rs_awakening = float(rs_line.iloc[-1]) > float(rs_ma20.iloc[-1])
        
        # 获取当前市场的 RS 排名 (百分位 0-100)
        current_rs_rank = float(rs_rank_series.iloc[-1]) if len(rs_rank_series) > 0 else 50
        
        # C. 紧致度 (收缩判定)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # D. 量能与口袋枢轴
        v_slice = vol.iloc[-11:-1].values
        c_slice = close.iloc[-11:-1].values
        c_prev_slice = close.iloc[-12:-2].values
        
        neg_vol_list = v_slice[c_slice < c_prev_slice]
        max_neg_vol = float(np.max(neg_vol_list)) if len(neg_vol_list) > 0 else 9e15
        
        is_pocket = (cp > float(close.iloc[-2])) and (float(vol.iloc[-1]) > max_neg_vol)
        
        vol_ma20 = float(vol.rolling(20).mean().iloc[-1])
        vol_ratio = float(vol.iloc[-1] / vol_ma20) if vol_ma20 > 0 else 0

        # =====================================
        # 👑 E. V1000 核心核武逻辑植入
        # =====================================
        
        # 【探测器 1：圣杯共振】 紧致度极低 + 贴近50日线 + 但相对强度极高
        is_singularity = (tightness < 2.5) and (current_rs_rank > 75) and (-2.0 <= ext_50 <= 2.0)
        
        # 【探测器 2：RS 价格背离】 近10日价格低迷(或横盘)，但 RS 创10日新高
        p_min_10 = float(close.iloc[-10:].min())
        rs_max_10 = float(rs_line.iloc[-10:-1].max())  # 过去9日的最高RS
        
        # 价格距离10日最低点不到 2% (说明在跌或死气沉沉)，但今天的 RS 突破了过去10日的前高
        is_price_weak = cp <= (p_min_10 * 1.02)
        is_rs_breakout = float(rs_line.iloc[-1]) > rs_max_10
        is_rs_divergence = is_price_weak and is_rs_breakout

        # =====================================
        # F. 动态评分与评级系统
        # =====================================
        score = 0
        signals =[]
        
        # 信号累加
        if is_singularity: signals.append("👑圣杯共振"); score += 8
        if is_rs_divergence: signals.append("★RS背离吸筹"); score += 6
        if is_pocket: signals.append("🎯口袋枢轴"); score += 4
        if rs_awakening: signals.append("🔔RS觉醒"); score += 3
        if tightness < 1.8: signals.append("👁️紧致"); score += 2
        if cp > ma10: score += 1

        # 状态判定 (僵尸股防御机制)
        is_zombie = (vol_ratio < 0.5) and not (is_singularity or is_rs_divergence or is_pocket)
        
        if is_singularity: 
            rating = "🏆 奇点觉醒 (圣杯)"
        elif is_rs_divergence:
            rating = "☢️ 机构暗吸 (背离)"
        elif is_zombie:
            rating = "🧟 缩量僵尸"
            score -= 3  # 降级处理
        elif is_bull and score >= 6: 
            rating = "💎 SSS 统帅"
        elif is_bull: 
            rating = "🔥 多头趋势"
        elif cp > ma10: 
            rating = "✅ 短线转强"
        else: 
            rating = "❄️ 均线压制"

        # 仓位控制 (ADR)
        adr = float(((high - low) / low).tail(20).mean() * 100)
        stop_p = max(ma50 * 0.98, cp * (1 - (adr * 0.01 * 1.5)))
        shares = (TOTAL_CAPITAL * MAX_RISK_PER_STOCK) // (cp - stop_p) if cp > stop_p else 0

        return {
            "Rating": rating, "Action": " + ".join(signals) if signals else "震荡/僵尸",
            "Price": cp, "Tightness": tightness, "Score": score,
            "Shares": int(shares), "Stop": stop_p, "is_bull": is_bull, 
            "Ext50": ext_50, "RSRank": current_rs_rank
        }
    except Exception as e:
        print(f"Error detail: {e}")
        return None

# ==========================================
# 🚀 3. 执行引擎
# ==========================================
def run_sentinel_commander():
    start_t = time.time()
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    print(f"🚀[{bj_now}] 启动 V1000 量子哨兵 [蓝筹绝对防御版]...")

    try:
        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', progress=False, threads=False)
        bench_raw = yf.download("^HSI", period="2y", progress=False, threads=False)
        bench_series = bench_raw['Close'].squeeze()
        if isinstance(bench_series, pd.DataFrame): bench_series = bench_series.iloc[:, 0]
        hsi_vol = float(bench_series.pct_change().tail(20).std() * math.sqrt(252) * 100)
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    # ---------------------------------------------------------
    # 🧠 构建全局 RS Rank 矩阵 (解决跨股票强度排名问题)
    # ---------------------------------------------------------
    all_closes = pd.DataFrame()
    for t in CORE_TICKERS_HK:
        if t in data.columns.levels[0]:
            all_closes[t] = data[t]['Close']
            
    # 计算全量 RS 并转换为 0-100 的百分位排名
    bench_aligned_global = bench_series.reindex(all_closes.index).ffill()
    rs_matrix = all_closes.div(bench_aligned_global, axis=0).ffill()
    rs_rank_matrix = rs_matrix.rank(axis=1, pct=True) * 100

    candidates =[]
    bull_count = 0
    
    for t in CORE_TICKERS_HK:
        try:
            df_t = data[t].dropna() if t in data.columns.levels[0] else None
            # 提取该股票的历史 RS Rank
            rs_rank_series = rs_rank_matrix[t].dropna() if t in rs_rank_matrix else pd.Series()
            
            res = calculate_sentinel_metrics(df_t, bench_series, rs_rank_series)
            
            if res:
                res["Ticker"] = t.replace(".HK", "")
                if res["is_bull"]: bull_count += 1
                
                # 剔除严重扣分的非核心僵尸股
                if (t in LEADER_WATCH) or (res["Score"] >= 0) or ("机构暗吸" in res["Rating"]):
                    candidates.append(res)
        except Exception as e: 
            print(f"处理 {t} 失败: {e}")
            continue

    # 排序：带有核武级别的标的强制置顶，其次看总分
    def sort_key(x):
        is_leader = 1 if (x['Ticker'] + ".HK") in LEADER_WATCH else 0
        is_holy = 1 if "圣杯" in x['Rating'] else 0
        is_diverge = 1 if "暗吸" in x['Rating'] else 0
        return (is_holy, is_diverge, is_leader, x['Score'])

    candidates.sort(key=sort_key, reverse=True)
    
    mkt_breadth = f"{round((bull_count / len(CORE_TICKERS_HK)) * 100, 1)}%"
    matrix = [["🏰 V1000 量子哨兵 [蓝筹绝对防御版]", "多头广度:", mkt_breadth, "北京时间:", bj_now, f"大盘波动: {round(hsi_vol,1)}%", "", "", "", ""],["代码", "哨兵评级", "核心信号", "建议股数", "现价", "止损参考", "全市场RS排名", "强度评分", "距离50MA", "紧致度"]
    ]

    for item in candidates[:25]:
        matrix.append([
            item["Ticker"], item["Rating"], item["Action"], item["Shares"], item["Price"],
            round(item["Stop"], 2), f"前 {100 - int(item['RSRank'])}%", item["Score"],
            f"{round(item['Ext50'], 1)}%", f"{round(item['Tightness'], 2)}%"
        ])

    try:
        def clean_final(v):
            if isinstance(v, (float, int)): return round(v, 2) if math.isfinite(v) else ""
            return str(v)
        
        final_matrix = [[clean_final(c) for c in r] for r in matrix]
        resp = requests.post(WEBAPP_URL, json=final_matrix, timeout=25)
        print(f"🎉 V1000 同步成功！检测出 {len([c for c in candidates if '圣杯' in c['Rating'] or '暗吸' in c['Rating']])} 个异动核武。多头广度: {mkt_breadth}")
    except Exception as e:
        print(f"❌ 网络同步失败: {e}")

if __name__ == "__main__":
    run_sentinel_commander()
