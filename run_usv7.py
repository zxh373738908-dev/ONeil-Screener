def sync_to_sheets_chunked(results, vix, spy_healthy):
    print("📝 正在激活潜龙切片同步引擎 (高可靠模式)...")
    
    # 1. 构建完整矩阵 (将 Header 和 Body 合并，减少 API 调用次数)
    bj_time = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    
    # 头部状态栏
    full_matrix = [
        ["🏰 V1000 终极枢纽 [8.0潜龙版]", "", "Update(BJ):", bj_time],
        ["市场环境:", "☀️ 激进" if (spy_healthy and vix < 21) else "⛈️ 避险", "VIX:", round(float(vix), 2), "SPY:", "健康" if spy_healthy else "弱势"],
        ["感知核心:", "👁️奇點先行(感知GOOGL/权重), 🚀巔峰突破(感知CF/资源), 💎SSS(终极共振)"],
        ["-" * 12] * 9  # 分割线
    ]
    
    # 加入数据体
    if results:
        df = pd.DataFrame(results)
        # 添加表头
        full_matrix.append(df.columns.tolist())
        # 添加数据行
        full_matrix.extend(df.values.tolist())
    else:
        full_matrix.append(["📭 当前战区进入静默期，无共振信号。"])

    # 数据净化 (统一处理所有单元格)
    cleaned_matrix = [[hidden_dragon_clean(cell) for cell in row] for row in full_matrix]

    # 2. 执行带重试机制的同步
    for attempt in range(1, 5): # 增加到 4 次尝试
        try:
            creds = Credentials.from_service_account_file(
                CREDS_FILE, 
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(SHEET_ID).worksheet(TARGET_SHEET_NAME)
            
            # --- 核心修复操作 ---
            # 1. 先清空目标区域（防止旧数据污染，也稳定 API 状态）
            sh.clear()
            time.sleep(1) 
            
            # 2. 一次性写入全部数据 (这是避免 Char 1 错误的关键：减少交互频率)
            # 使用 range_name='A1' 会自动根据矩阵大小扩展
            sh.update('A1', cleaned_matrix)
            
            print(f"🎉 潜龙同步成功！(尝试第 {attempt} 次)")
            return # 成功后退出
            
        except Exception as e:
            wait_time = attempt * 30 # 递增等待：30s, 60s, 90s
            print(f"⚠️ 同步异常 (尝试 {attempt}/4): {e}")
            if "char 1" in str(e).lower():
                print(f"💡 检测到 Google API 频率限制，进入深度静默 {wait_time} 秒...")
            time.sleep(wait_time)

    print("❌ 经过多次尝试，无法连接至 Google Sheets，请检查网络或 API 额度。")
