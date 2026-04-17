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
        
        # A. 均线与偏离度 (Ext_50)
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

        # E. V1000 核武探测器
        is_singularity = (tightness < 2.5) and (current_rs_rank > 75) and (-2.0 <= ext_50 <= 2.0)
        
        p_min_10 = float(close.iloc[-10:].min())
        rs_max_10 = float(rs_line.iloc[-10:-1].max())  
        is_price_weak = cp <= (p_min_10 * 1.02)
        is_rs_breakout = float(rs_line.iloc[-1]) > rs_max_10
        is_rs_divergence = is_price_weak and is_rs_breakout

        # F. 动态评分与评级
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
        return None

# ==========================================
# 🚀 3. 执行引擎 (多维矩阵极速版)
# ==========================================
def run_sentinel_commander():
    start_t = time.time()
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    print(f"🚀[{bj_now}] 启动 V1001 量子哨兵 [极速防御版]...")

    try:
        # 优化：取消 group_by='ticker'，直接拉取全局 DataFrame（极大提升兼容性和速度）
        data = yf.download(CORE_TICKERS_HK, period="2y", progress=False, threads=False)
        bench_raw = yf.download("^HSI", period="2y", progress=False, threads=False)
        bench_series = bench_raw['Close'].squeeze()
        if isinstance(bench_series, pd.DataFrame): bench_series = bench_series.iloc[:, 0]
        hsi_vol = float(bench_series.pct_change().tail(20).std() * math.sqrt(252) * 100)
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    # 构建全局 RS Rank 矩阵
    # 提取所有收盘价，兼容多列索引
    all_closes = data['Close'] if isinstance(data.columns, pd.MultiIndex) else pd.DataFrame(data['Close'], columns=CORE_TICKERS_HK)
    all_closes = all_closes.ffill()

    bench_aligned_global = bench_series.reindex(all_closes.index).ffill()
    rs_matrix = all_closes.div(bench_aligned_global, axis=0).ffill()
    rs_rank_matrix = rs_matrix.rank(axis=1, pct=True) * 100

    candidates =[]
    bull_count = 0
    
    for t in CORE_TICKERS_HK:
        try:
            # 优化：安全提取单只股票数据结构，避免复杂的 MultiIndex 报错
            if t not in all_closes.columns: continue
            
            df_t = pd.DataFrame({
                'Close': data['Close'][t],
                'High': data['High'][t],
                'Low': data['Low'][t],
                'Volume': data['Volume'][t]
            }).dropna()

            rs_rank_series = rs_rank_matrix[t].dropna() if t in rs_rank_matrix.columns else pd.Series(dtype=float)
            
            res = calculate_sentinel_metrics(df_t, bench_series, rs_rank_series)
            
            if res:
                res["Ticker"] = t.replace(".HK", "")
                if res["is_bull"]: bull_count += 1
                
                if (t in LEADER_WATCH) or (res["Score"] >= 0) or ("机构暗吸" in res["Rating"]):
                    candidates.append(res)
        except Exception as e: 
            continue

    def sort_key(x):
        is_leader = 1 if (x['Ticker'] + ".HK") in LEADER_WATCH else 0
        is_holy = 1 if "圣杯" in x['Rating'] else 0
        is_diverge = 1 if "暗吸" in x['Rating'] else 0
        return (is_holy, is_diverge, is_leader, x['Score'])

    candidates.sort(key=sort_key, reverse=True)
    
    mkt_breadth = f"{round((bull_count / len(CORE_TICKERS_HK)) * 100, 1)}%"
    matrix = [
        ["🏰 V1001 量子哨兵 [极速防御版]", "多头广度:", mkt_breadth, "北京时间:", bj_now, f"大盘波动: {round(hsi_vol,1)}%", "", "", "", ""],["代码", "哨兵评级", "核心信号", "建议股数", "现价", "止损参考", "全市场RS排名", "强度评分", "距离50MA", "紧致度"]
    ]

    # 🚨 核心优化：强制所有数字转为格式化纯文本 String，彻底阻断 Google 表格的 "41120%" 显示 Bug
    for item in candidates[:25]:
        matrix.append([
            item["Ticker"], 
            item["Rating"], 
            item["Action"], 
            f'{item["Shares"]}',                 # 转为纯文本形式
            f'{item["Price"]:.2f}',              # 锁定两位小数文本
            f'{item["Stop"]:.2f}',               # 锁定两位小数文本
            f"前 {100 - int(item['RSRank'])}%",   
            f'{item["Score"]}',                  # 锁定纯数字文本
            f'{item["Ext50"]:.1f}%',             
            f'{item["Tightness"]:.2f}%'          
        ])

    try:
        # 直接发送处理好的安全矩阵，不再进行容易产生类型歧义的 round 转换
        resp = requests.post(WEBAPP_URL, json=matrix, timeout=25)
        print(f"🎉 V1001 极速版同步成功！用时: {round(time.time() - start_t, 2)}秒 | 检测出核武: {len([c for c in candidates if '圣杯' in c['Rating'] or '暗吸' in c['Rating']])} 只")
    except Exception as e:
        print(f"❌ 网络同步失败: {e}")

if __name__ == "__main__":
    run_sentinel_commander()
