# ==========================================
# 3. 核心算法：V1000 枢纽共振逻辑 (增强感知版)
# ==========================================
def calculate_v1000_nexus(df, spy_df):
    try:
        if len(df) < 150: return None
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = close.iloc[-1]
        
        # 均线与成交量
        ma50 = close.rolling(50).mean().iloc[-1]
        vol_ma50 = vol.rolling(50).mean().iloc[-1]

        # 相对强度 (RS) 逻辑
        rs_line = close / spy_df
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max()
        # 紧致度 (VCP感知)：降低门槛到 2.0，核心信号仍需 1.3
        tightness = (close.tail(10).std() / close.tail(10).mean()) * 100
        
        # RS 性能评分
        def get_perf(d): return (curr_price - close.iloc[-d]) / close.iloc[-d]
        rs_score = (get_perf(63)*2 + get_perf(126) + get_perf(189))

        signals, base_res = [], 0
        
        # 1. 👁️ 奇点先行 (感知 RS 爆发 + 极度紧致)
        if rs_nh_20:
            if tightness < 1.4:
                signals.append("👁️奇點先行")
                base_res += 4
            else:
                signals.append("📈RS走强")
                base_res += 1
            
        # 2. 🚀 巅峰突破 (股价接近一年高点 + 放量)
        high_52w = high.tail(252).max()
        if curr_price >= high_52w * 0.97:
            if vol.iloc[-1] > vol_ma50:
                signals.append("🚀巔峰突破")
                base_res += 3
            else:
                signals.append("🔭临近高位")
                base_res += 1
            
        # 3. 🐉 老龙回头 (强力标的回踩 50MA)
        if rs_score > 0.3 and abs(curr_price - ma50)/ma50 < 0.04:
            signals.append("🐉老龍回頭")
            base_res += 2

        # 即使没有任何信号，也返回数据，只要它的 RS 强度够高
        if not signals and rs_score < 0.2: return None
        
        adr = ((high - low) / low).tail(20).mean() * 100
        
        return {
            "RS_Score": rs_score, 
            "Signals": " + ".join(signals) if signals else "📊趋势震荡", 
            "Base_Res": base_res, 
            "Price": curr_price, 
            "Tightness": tightness, 
            "ADR": adr
        }
    except: return None

# ==========================================
# 5. 主执行流程 (保持 10秒内完成)
# ==========================================
def run_v1000_nexus_bridge():
    start_time = time.time()
    print("🏟️ V1000 枢纽系统 [增强感知版] 启动...")
    
    # 1. 下载数据
    env = yf.download(["SPY", "^VIX"], period="1y", progress=False)['Close']
    spy_df = env['SPY'].dropna(); vix = env['^VIX'].iloc[-1]
    data = yf.download(CORE_TICKERS, period="1y", group_by='ticker', threads=True, progress=False)
    
    candidates = []
    for t in CORE_TICKERS:
        try:
            res = calculate_v1000_nexus(data[t].dropna(), spy_df)
            if res:
                res["Ticker"] = t
                candidates.append(res)
        except: continue

    # 2. 排序逻辑：Base_Res(信号分) 优先，RS_Score(强度) 次之
    if not candidates:
        print("📭 全市场进入静默期，无强势标的。")
        # 即使没结果也发个“空状态”给表格，确认系统活着
        results = [["-", "😴静默", "全市场无共振", "-", "-", "-", "-", "-"]]
    else:
        # 按得分排序，取 Top 12
        final_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(12)
        results = []
        
        print(f"🔥 发现 {len(final_df)} 只活跃标的，执行期权穿透...")
        for i, row in final_df.reset_index().iterrows():
            opt_call = "N/A"
            # 只有前 3 名且得分 >= 2 的标的才去 Polygon 查期权，节省配额和时间
            if i < 3 and row['Base_Res'] >= 2:
                opt_call = get_option_audit(row['Ticker'])
            
            rating = "💎SSS" if row['Base_Res'] >= 5 else "🔥强势" if row['Base_Res'] >= 3 else "✅监控"
            
            results.append([
                row['Ticker'],
                rating,
                row['Signals'],
                opt_call,
                row['Price'],
                f"{round(row['Tightness'],2)}%",
                round(row['RS_Score'], 2),
                f"{round(row['ADR'],2)}%"
            ])

    # 3. 构造矩阵发送
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🏰 V1000 枢纽 (8.0感应版)", "Update:", bj_now, "VIX:", round(vix, 2)],
        ["代码", "评级", "枢纽信号 (共振感知)", "看涨% (Top3)", "现价", "紧致度", "RS强度", "ADR"]
    ]
    matrix = header + results

    # 4. 上传
    try:
        resp = requests.post(WEBAPP_URL, data=json.dumps(matrix), timeout=10)
        print(f"🎉 任务达成！耗时: {round(time.time() - start_time, 2)}s")
    except:
        print("❌ 桥接失败")
