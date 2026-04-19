import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
# 使用你最新提供的 WebApp URL
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxyU65vHlyMg-RvIEz9TyXRDLT-M830EIAFUZIjsicSooiLnkmkMVNfaC5ccSLiDD7t/exec"

TOTAL_CAPITAL = 1000000 
MAX_RISK_PER_STOCK = 0.008 

# 核心监控（必出标的）
LEADER_WATCH =["0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK"]

# 股票池
CORE_TICKERS_HK = list(set(LEADER_WATCH +[
    "0941.HK", "2318.HK", "0005.HK", "9999.HK", "0883.HK",
    "1024.HK", "1299.HK", "2015.HK", "9618.HK", "0939.HK",
    "1398.HK", "2331.HK", "2020.HK", "1177.HK", "2269.HK", "0388.HK"
]))

# ==========================================
# 🧠 2. 量子哨兵演算法 (V54.1 Pro-Layout)
# ==========================================
def calculate_sentinel_metrics(df, hsi_series, rs_rank_series):
    try:
        if df is None or df.empty or len(df) < 60: return None
        
        close = df['Close'].ffill()
        high = df['High'].ffill()
        low = df['Low'].ffill()
        vol = df['Volume'].ffill()
        cp = float(close.iloc[-1])
        
        # A. 均线与偏离度 (Bias/Ext_50) & 60D Trend
        ma10 = float(close.rolling(10).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        
        ma60 = float(close.rolling(60).mean().iloc[-1]) if len(close) >= 60 else cp
        ma60_prev = float(close.rolling(60).mean().iloc[-5]) if len(close) >= 65 else ma60
        # 60日趋势判定
        trend_60d = "↑ 向上" if ma60 > ma60_prev else ("↓ 向下" if ma60 < ma60_prev else "→ 走平")

        is_bull = cp > ma50
        ext_50 = ((cp - ma50) / ma50) * 100 if ma50 > 0 else 0
        
        # B. 横向与纵向 RS 数据
        bench_aligned = hsi_series.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        if len(rs_line) < 20: return None
        
        rs_ma20 = rs_line.rolling(20).mean()
        rs_awakening = float(rs_line.iloc[-1]) > float(rs_ma20.iloc[-1])
        
        current_rs_rank = float(rs_rank_series.iloc[-1]) if not rs_rank_series.empty else 50
        
        # C. 紧致度 & ADR
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100) if close.tail(10).mean() > 0 else 0
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

        # E. 探测器
        is_singularity = (tightness < 2.5) and (current_rs_rank > 75) and (-2.0 <= ext_50 <= 2.0)
        p_min_10 = float(close.iloc[-10:].min())
        rs_max_10 = float(rs_line.iloc[-10:-1].max())  
        is_price_weak = cp <= (p_min_10 * 1.02)
        is_rs_breakout = float(rs_line.iloc[-1]) > rs_max_10
        is_rs_divergence = is_price_weak and is_rs_breakout

        # F. 动态评分与信号
        score = 0
        signals =[]
        if is_singularity: signals.append("👑圣杯"); score += 8
        if is_rs_divergence: signals.append("★背离"); score += 6
        if is_pocket: signals.append("🎯口袋"); score += 4
        if rs_awakening: signals.append("🔔觉醒"); score += 3
        if tightness < 1.8: signals.append("👁️紧致"); score += 2
        if cp > ma10: score += 1

        is_zombie = (vol_ratio < 0.5) and not (is_singularity or is_rs_divergence or is_pocket)
        
        if is_singularity: rating = "🏆 奇点觉醒"
        elif is_rs_divergence: rating = "☢️ 机构暗吸"
        elif is_zombie: rating = "🧟 缩量僵尸"; score -= 3
        elif is_bull and score >= 6: rating = "💎 SSS统帅"
        elif is_bull: rating = "🔥 多头趋势"
        elif cp > ma10: rating = "✅ 短线转强"
        else: rating = "❄️ 均线压制"

        # G. 绝对与相对动量计算 (预防分母为0)
        def get_pct(series, period):
            if len(series) > period and series.iloc[-period-1] > 0:
                return float(series.pct_change(period).iloc[-1] * 100)
            return 0.0

        ret_1d   = get_pct(close, 1)
        ret_5d   = get_pct(close, 5)
        ret_20d  = get_pct(close, 20)
        ret_60d  = get_pct(close, 60)
        ret_120d = get_pct(close, 120)
        
        rs_20d  = get_pct(rs_line, 20)
        rs_60d  = get_pct(rs_line, 60)
        rs_120d = get_pct(rs_line, 120)

        # H. 计算 YTD (From 2024-12-31 至今收益率)
        try:
            historical_close = close.loc[close.index.year <= 2024]
            if not historical_close.empty:
                p_2024_end = float(historical_close.iloc[-1])
                ret_2024 = ((cp - p_2024_end) / p_2024_end) * 100 if p_2024_end > 0 else 0.0
            else:
                ret_2024 = 0.0
        except:
            ret_2024 = 0.0

        return {
            "Rating": rating, "Action": " + ".join(signals) if signals else "震荡",
            "Price": cp, "Tightness": tightness, "Score": score,
            "is_bull": is_bull, "Ext50": ext_50, "RSRank": current_rs_rank,
            "ADR": adr, "Vol_Ratio": vol_ratio, "Trend60": trend_60d,
            "1D": ret_1d, "5D": ret_5d, "20D": ret_20d, "60D": ret_60d, "120D": ret_120d,
            "R20": rs_20d, "R60": rs_60d, "R120": rs_120d,
            "Ret2024": ret_2024
        }
    except Exception as e:
        return None

# ==========================================
# 行业与市值聚合查询器
# ==========================================
def get_extended_info(ticker_str):
    try:
        tkr = yf.Ticker(ticker_str)
        mcap = tkr.fast_info.get("marketCap", 0) / 1e9
        industry = tkr.info.get("industry", "N/A")
        return mcap, industry
    except:
        return 0.0, "N/A"

# ==========================================
# 🚀 3. 执行引擎 (终极校验制导版)
# ==========================================
def run_sentinel_commander():
    start_t = time.time()
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    print(f"🚀[{bj_now}] 启动 V54.1 Pro-Layout 优化版...")

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
            
            res = calculate_sentinel_metrics(df_t, bench_series, rs_rank_series)
            
            if res:
                mcap, industry = get_extended_info(t)
                res["Ticker"] = t.replace(".HK", "")
                res["MktCap"] = mcap
                res["Industry"] = industry
                
                if res["is_bull"]: bull_count += 1
                
                # 入围门槛
                if (t in LEADER_WATCH) or (res["Score"] >= 0) or ("机构暗吸" in res["Rating"]):
                    candidates.append(res)
        except Exception as e: 
            continue

    # 排序：圣杯 > 暗吸背离 > 核心观察池 > 强度得分
    def sort_key(x):
        is_leader = 1 if (x['Ticker'] + ".HK") in LEADER_WATCH else 0
        is_holy = 1 if "圣杯" in x['Rating'] else 0
        is_diverge = 1 if "暗吸" in x['Rating'] else 0
        return (is_holy, is_diverge, is_leader, x['Score'])

    candidates.sort(key=sort_key, reverse=True)
    
    mkt_breadth = f"{round((bull_count / len(CORE_TICKERS_HK)) * 100, 1)}%"
    
    # ==========================================
    # 🎯 构建超级矩阵 (严格保证 21 列)
    # ==========================================
    # 第一行(Title): 6列文字 + 15列空字符串 = 21列
    title_row =["🏰 V54.1 Pro-Layout优化版", f"大盘波动: {round(hsi_vol,1)}%", "多头广度:", mkt_breadth, "北京时间:", bj_now] + [""] * 15
    
    # 第二行(Header): 严格对应的 21 列
    header_row =[
        "Ticker", "Industry", "Score", "1D%", "60D Trend", "Action", "Resonance", 
        "ADR", "Vol_Ratio", "Bias", "MktCap", "Rank", "REL5", "REL20", "REL60", 
        "REL120", "R20", "R60", "R120", "Price", "From 2024-12-31"
    ]
    
    matrix =[title_row, header_row]

    # 第三行开始(Data): 锁定50只标的，灌注格式化好的21列数据
    for item in candidates[:50]:
        matrix.append([
            item["Ticker"],                            # 1. 股票代码
            item["Industry"],                          # 2. 行业
            f'{item["Score"]}',                        # 3. 得分
            f'{item["1D"]:.2f}%',                      # 4. 1日收益
            item["Trend60"],                           # 5. 60日线趋势
            item["Action"],                            # 6. 买点动作
            item["Rating"],                            # 7. 共振信号
            f'{item["ADR"]:.2f}%',                     # 8. 波动率
            f'{item["Vol_Ratio"]:.2f}',                # 9. 量比
            f'{item["Ext50"]:.2f}%',                   # 10. 均线乖离
            f'{item["MktCap"]:.1f}B',                  # 11. 市值
            f'{int(item["RSRank"])}',                  # 12. RS排名
            f'{item["5D"]:.2f}%',                      # 13. 5日收益
            f'{item["20D"]:.2f}%',                     # 14. 20日收益
            f'{item["60D"]:.2f}%',                     # 15. 60日收益
            f'{item["120D"]:.2f}%',                    # 16. 120日收益
            f'{item["R20"]:.2f}%',                     # 17. RS 20日动量
            f'{item["R60"]:.2f}%',                     # 18. RS 60日动量
            f'{item["R120"]:.2f}%',                    # 19. RS 120日动量
            f'{item["Price"]:.2f}',                    # 20. 最新价
            f'{item["Ret2024"]:.2f}%'                  # 21. YTD 跨年收益
        ])

    # ==========================================
    # 🚨 终极同步与排错防弹衣
    # ==========================================
    # 1. 严格检查矩阵形状（防 Sheet 报错拦截）
    col_counts = set(len(row) for row in matrix)
    if len(col_counts) > 1:
        print(f"❌ 严重错误：矩阵列数不一致，导致Google Sheet直接崩溃！")
        print(f"目前矩阵中存在的列数分布: {col_counts}。必须完全一致（如全部是21）。")
        return 

    # 2. 检查非法浮点数拦截
    try:
        clean_json = json.dumps(matrix, allow_nan=False)
    except ValueError as e:
        print(f"❌ 严重错误：数据存在 NaN 或 Infinity 无法转为 JSON: {e}")
        return

    # 3. 真实网络发送与谷歌层级错误解析
    try:
        resp = requests.post(WEBAPP_URL, json=matrix, timeout=30, allow_redirects=True)
        response_text = resp.text.strip()
        
        # 验证返回内容是否属于正常状态
        if resp.status_code == 200 and "<html" not in response_text[:20].lower() and "Exception:" not in response_text:
            print(f"✅ V54.1 间距优化版同步完成。用时: {round(time.time() - start_t, 2)}秒 | 锁定 {len(candidates[:50])} 只标的。")
            print(f"⏱️ 更新时间截面: {bj_now}")
            # ★ 终极诊断：把谷歌的底层回复完整打出来
            print(f"📡 谷歌服务器底层回传: {response_text}")
        else:
            print(f"⚠️ 警告: 网络已连接，但 Google Sheet 端可能写入崩溃或失败！")
            print(f"HTTP 状态码: {resp.status_code}")
            print(f"Google 真实报错截获: \n{response_text[:300]}")
            print(f"-> 提示：如果看到 'Exception: The number of columns in the data does not match'，请手动打开 Google Sheet，往右侧多插入几个空白列，确保总列数大于等于 21 列。")
            
    except Exception as e:
        print(f"❌ 网络请求或同步发生致命错误: {e}")

if __name__ == "__main__":
    run_sentinel_commander()
