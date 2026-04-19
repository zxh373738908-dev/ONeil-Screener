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
# 🧠 2. 量子哨兵演算法 (V1001 极速稳定版)
# ==========================================
def calculate_sentinel_metrics(df, hsi_series, rs_rank_series):
    try:
        if df is None or df.empty or len(df) < 60: return None
        
        close = df['Close'].ffill()
        high = df['High'].ffill()
        low = df['Low'].ffill()
        vol = df['Volume'].ffill()
        cp = float(close.iloc[-1])
        
        # A. 均线与偏离度 (Bias/Ext_50)
        ma10 = float(close.rolling(10).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        is_bull = cp > ma50
        ext_50 = ((cp - ma50) / ma50) * 100 
        
        # B. 横向与纵向 RS 数据
        bench_aligned = hsi_series.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        if len(rs_line) < 20: return None
        
        rs_ma20 = rs_line.rolling(20).mean()
        rs_awakening = float(rs_line.iloc[-1]) > float(rs_ma20.iloc[-1])
        
        current_rs_rank = float(rs_rank_series.iloc[-1]) if not rs_rank_series.empty else 50
        
        # C. 紧致度 & ADR
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        # D. 量能与口袋枢轴
        v_slice = vol.iloc[-11:-1].values
        c_slice = close.iloc[-11:-1].values
        c_prev_slice = close.iloc[-12:-2].values
        
        neg_vol_list = v_slice[c_slice < c_prev_slice]
        max_neg_vol = float(np.max(neg_vol_list)) if len(neg_vol_list) > 0 else 9e15
        
        is_pocket = (cp > float(close.iloc[-2])) and (float(vol.iloc[-1]) > max_neg_vol)
        
        vol_ma20 = float(vol.rolling(20).mean().iloc[-1])
        vol_ratio = float(vol.iloc[-1] / vol_ma20) if vol_ma20 > 0 else 0

        # E. V1000 核武探测器
        is_singularity = (tightness < 2.5) and (current_rs_rank > 75) and (-2.0 <= ext_50 <= 2.0)
        
        p_min_10 = float(close.iloc[-10:].min())
        rs_max_10 = float(rs_line.iloc[-10:-1].max())  
        is_price_weak = cp <= (p_min_10 * 1.02)
        is_rs_breakout = float(rs_line.iloc[-1]) > rs_max_10
        is_rs_divergence = is_price_weak and is_rs_breakout

        # F. 动态评分与信号 (Resonance / Action)
        score = 0
        signals =[]
        
        if is_singularity: signals.append("👑圣杯共振"); score += 8
        if is_rs_divergence: signals.append("★RS背离"); score += 6
        if is_pocket: signals.append("🎯口袋枢轴"); score += 4
        if rs_awakening: signals.append("🔔RS觉醒"); score += 3
        if tightness < 1.8: signals.append("👁️紧致"); score += 2
        if cp > ma10: score += 1

        is_zombie = (vol_ratio < 0.5) and not (is_singularity or is_rs_divergence or is_pocket)
        
        if is_singularity: rating = "🏆 奇点觉醒 (圣杯)"
        elif is_rs_divergence: rating = "☢️ 机构暗吸 (背离)"
        elif is_zombie: rating = "🧟 缩量僵尸"; score -= 3
        elif is_bull and score >= 6: rating = "💎 SSS 统帅"
        elif is_bull: rating = "🔥 多头趋势"
        elif cp > ma10: rating = "✅ 短线转强"
        else: rating = "❄️ 均线压制"

        # G. 绝对与相对动量矩阵计算 (5D, 20D, 60D, R20, R60)
        ret_5d  = float(close.pct_change(5).iloc[-1] * 100) if len(close) > 5 else 0
        ret_20d = float(close.pct_change(20).iloc[-1] * 100) if len(close) > 20 else 0
        ret_60d = float(close.pct_change(60).iloc[-1] * 100) if len(close) > 60 else 0
        
        rs_20d  = float(rs_line.pct_change(20).iloc[-1] * 100) if len(rs_line) > 20 else 0
        rs_60d  = float(rs_line.pct_change(60).iloc[-1] * 100) if len(rs_line) > 60 else 0

        # 风控计算
        stop_p = max(ma50 * 0.98, cp * (1 - (adr * 0.01 * 1.5)))
        shares = (TOTAL_CAPITAL * MAX_RISK_PER_STOCK) // (cp - stop_p) if cp > stop_p else 0

        return {
            "Rating": rating, "Action": " + ".join(signals) if signals else "震荡/无信号",
            "Price": cp, "Tightness": tightness, "Score": score,
            "Shares": int(shares), "Stop": stop_p, "is_bull": is_bull, 
            "Ext50": ext_50, "RSRank": current_rs_rank,
            "ADR": adr, "Vol_Ratio": vol_ratio,
            "5D": ret_5d, "20D": ret_20d, "60D": ret_60d,
            "R20": rs_20d, "R60": rs_60d
        }
    except Exception as e:
        return None

# ==========================================
# 极速获取市值 (防御性函数)
# ==========================================
def get_market_cap_billions(ticker_str):
    try:
        # 使用 fast_info 拒绝下载庞大 JSON，提升速度 10 倍
        mcap = yf.Ticker(ticker_str).fast_info.get("marketCap", 0)
        return mcap / 1e9
    except:
        return 0.0

# ==========================================
# 🚀 3. 执行引擎 (多维矩阵极速版)
# ==========================================
def run_sentinel_commander():
    start_t = time.time()
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    print(f"🚀[{bj_now}] 启动 V1001 量子哨兵 [极速全维版]...")

    try:
        data = yf.download(CORE_TICKERS_HK, period="2y", progress=False, threads=False)
        bench_raw = yf.download("^HSI", period="2y", progress=False, threads=False)
        bench_series = bench_raw['Close'].squeeze()
        if isinstance(bench_series, pd.DataFrame): bench_series = bench_series.iloc[:, 0]
        hsi_vol = float(bench_series.pct_change().tail(20).std() * math.sqrt(252) * 100)
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    # 构建全局 RS Rank 矩阵
    all_closes = data['Close'] if isinstance(data.columns, pd.MultiIndex) else pd.DataFrame(data['Close'], columns=CORE_TICKERS_HK)
    all_closes = all_closes.ffill()

    bench_aligned_global = bench_series.reindex(all_closes.index).ffill()
    rs_matrix = all_closes.div(bench_aligned_global, axis=0).ffill()
    rs_rank_matrix = rs_matrix.rank(axis=1, pct=True) * 100

    candidates =[]
    bull_count = 0
    
    for t in CORE_TICKERS_HK:
        try:
            if t not in all_closes.columns: continue
            
            df_t = pd.DataFrame({
                'Close': data['Close'][t],
                'High': data['High'][t],
                'Low': data['Low'][t],
                'Volume': data['Volume'][t]
            }).dropna()

            rs_rank_series = rs_rank_matrix[t].dropna() if t in rs_rank_matrix.columns else pd.Series(dtype=float)
            
            # 核心计算
            res = calculate_sentinel_metrics(df_t, bench_series, rs_rank_series)
            
            if res:
                # 注入额外信息
                res["Ticker"] = t.replace(".HK", "")
                res["MktCap"] = get_market_cap_billions(t)
                
                if res["is_bull"]: bull_count += 1
                
                # 入围门槛
                if (t in LEADER_WATCH) or (res["Score"] >= 0) or ("机构暗吸" in res["Rating"]):
                    candidates.append(res)
        except Exception as e: 
            continue

    # 排序逻辑：圣杯 > 暗吸背离 > 核心观察池 > 强度得分
    def sort_key(x):
        is_leader = 1 if (x['Ticker'] + ".HK") in LEADER_WATCH else 0
        is_holy = 1 if "圣杯" in x['Rating'] else 0
        is_diverge = 1 if "暗吸" in x['Rating'] else 0
        return (is_holy, is_diverge, is_leader, x['Score'])

    candidates.sort(key=sort_key, reverse=True)
    
    mkt_breadth = f"{round((bull_count / len(CORE_TICKERS_HK)) * 100, 1)}%"
    
    # 🚨 Header构建，确保前后列数完全一致（15列）
    matrix = [
        ["🏰 V1001 量子哨兵[全维重构版]", f"大盘波动: {round(hsi_vol,1)}%", "多头广度:", mkt_breadth, "北京时间:", bj_now, "", "", "", "", "", "", "", "", ""],["代码", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", "Price", "5D", "20D", "60D", "R20", "R60"]
    ]

    # 格式化灌注数据，阻断 Google Sheet 数字解析 BUG
    for item in candidates[:30]:
        matrix.append([
            item["Ticker"], 
            f'{item["Score"]}',                    # 强度评分
            item["Action"],                        # 核心信号
            item["Rating"],                        # 共振评级
            f'{item["ADR"]:.2f}%',                 # ADR波动率
            f'{item["Vol_Ratio"]:.2f}',            # 量比
            f'{item["Ext50"]:.2f}%',               # 乖离率 (Bias)
            f'{item["MktCap"]:.1f}B',              # 市值 (十亿港币)
            f'{int(item["RSRank"])}',              # RS 相对排名分数(1-100)
            f'{item["Price"]:.2f}',                # 现价
            f'{item["5D"]:.2f}%',                  # 5日绝对收益
            f'{item["20D"]:.2f}%',                 # 20日绝对收益
            f'{item["60D"]:.2f}%',                 # 60日绝对收益
            f'{item["R20"]:.2f}%',                 # RS线20日变动(超额动量)
            f'{item["R60"]:.2f}%'                  # RS线60日变动(超额动量)
        ])

    try:
        resp = requests.post(WEBAPP_URL, json=matrix, timeout=25)
        print(f"🎉 V1001 全维矩阵同步成功！用时: {round(time.time() - start_t, 2)}秒 | 入围标的: {len(candidates)} 只")
    except Exception as e:
        print(f"❌ 网络同步失败: {e}")

if __name__ == "__main__":
    run_sentinel_commander()
