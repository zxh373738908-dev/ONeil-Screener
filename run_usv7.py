# ==========================================
# 2. 核心算法逻辑 (9.1 灵动感知版)
# ==========================================
def calculate_v1000_nexus(df, spy_df):
    try:
        if len(df) < 150: return None
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = close.iloc[-1]
        ma50 = close.rolling(50).mean().iloc[-1]
        vol_ma50 = vol.rolling(50).mean().iloc[-1]

        # 1. 相对强度 (RS) 逻辑
        rs_line = close / spy_df
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max()
        
        # 2. 紧致度 (VCP感知)
        tightness = (close.tail(10).std() / close.tail(10).mean()) * 100
        
        # 3. RS 性能评分 (加权计算：3个月, 6个月, 12个月)
        def get_perf(d): return (curr_price - close.iloc[-d]) / close.iloc[-d]
        rs_score = (get_perf(63)*2.5 + get_perf(126)*1.5 + get_perf(250))

        signals, base_res = [], 0
        
        # --- 信号判定逻辑 ---
        # A. 奇点/趋势信号
        if rs_nh_20:
            if tightness < 1.6: # 略微放宽到 1.6
                signals.append("👁️奇點")
                base_res += 4
            else:
                signals.append("📈趋势")
                base_res += 1
                
        # B. 巅峰突破信号
        high_52w = high.tail(252).max()
        if curr_price >= high_52w * 0.97:
            if vol.iloc[-1] > vol_ma50:
                signals.append("🚀突破")
                base_res += 3
            else:
                signals.append("🔭临界")
                base_res += 1
                
        # C. 老龙回头 (强力股回踩)
        if rs_score > 0.4 and abs(curr_price - ma50)/ma50 < 0.04:
            signals.append("🐉回頭")
            base_res += 2

        # 核心改进：即使没有特殊信号，只要 RS_Score > 0 也会返回，保证表格不为空
        if rs_score < -0.1: return None 
        
        adr = ((high - low) / low).tail(20).mean() * 100
        return {
            "RS_Score": rs_score, 
            "Signals": signals, 
            "Base_Res": base_res, 
            "Price": curr_price, 
            "Tightness": tightness, 
            "ADR": adr
        }
    except: return None

# ==========================================
# 3. 主指挥流程 (9.1 灵动感知版)
# ==========================================
def run_v1000_final():
    start_time = time.time()
    print("🚀 V1000 枢纽系统 [9.1灵动版] 启动...")

    # 1. 数据下载 (增加下载成功检查)
    data = yf.download(CORE_TICKERS, period="1y", group_by='ticker', threads=True, progress=False)
    spy_df = yf.download("SPY", period="1y", progress=False)['Close'].dropna()
    vix_df = yf.download("^VIX", period="5d", progress=False)['Close']
    vix = vix_df.iloc[-1] if not vix_df.empty else 20.0

    candidates = []
    sector_cluster = {}
    
    # 2. 核心演算
    for t in CORE_TICKERS:
        try:
            df_t = data[t].dropna()
            if df_t.empty: continue
            res = calculate_v1000_nexus(df_t, spy_df)
            if res:
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP.get(t, "其他")
                candidates.append(res)
                # 只要有上涨趋势，就计入板块集群
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 极端行情：未发现任何多头迹象。")
        return

    # 3. 排序与审计 (确保前 12 名一定能显示)
    # 排序逻辑：信号得分(Base_Res) > RS强度(RS_Score)
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(12)
    
    final_list = []
    print(f"🔥 发现 {len(sorted_df)} 只多头候选，执行期权与板块审计...")
    
    for i, row in sorted_df.reset_index().iterrows():
        cluster_count = sector_cluster.get(row['Sector'], 1)
        # 综合分：信号分 + 强度权重 + 集群奖励
        total_score = row['Base_Res'] + (1 if cluster_count >= 2 else 0)
        
        # 期权审计 (仅对前 2 名且综合分较高的)
        opt_call = "N/A"
        if i < 2 and total_score >= 1:
            opt_call = get_option_audit(row['Ticker'])
            time.sleep(0.5)

        # 评级逻辑：更细致的分类
        if total_score >= 5: rating = "💎SSS 共振"
        elif total_score >= 3: rating = "🔥强势"
        elif row['RS_Score'] > 0.5: rating = "🚀高动能"
        else: rating = "✅监控"
        
        final_list.append([
            row['Ticker'],
            rating,
            " + ".join(row['Signals']) if row['Signals'] else "📊 蓄势中",
            f"{cluster_count}只异动",
            opt_call,
            round(row['Price'], 2),
            f"{round(row['Tightness'], 2)}%",
            round(row['RS_Score'], 2),
            f"{round(row['ADR'], 2)}%",
            row['Sector']
        ])

    # 4. 同步至 Google Sheets
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🏰 V1000 终极枢纽 (9.1版)", "Update:", bj_now, "VIX:", round(vix, 2), "", "", "", "", ""],
        ["代码", "评级", "枢纽信号", "板块集群", "看涨% (Top2)", "现价", "紧致度", "RS强度", "ADR", "板块"]
    ]
    
    matrix = header + final_list
    
    try:
        resp = requests.post(WEBAPP_URL, data=json.dumps(matrix), headers={'Content-Type': 'application/json'}, timeout=15)
        print(f"🎉 任务达成！表格已更新。耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v1000_final()
