def final_output(res, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        sh = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        
        # 1. 顶部状态栏颜色优化
        weather_color = {"red": 0.8, "green": 1.0, "blue": 0.8} if "☀️" in weather else {"red": 1.0, "green": 0.9, "blue": 0.7}
        
        header = [
            ["🏰 [V750 巅峰 7.5 - 国际标准版]", "", "Update(北京):", bj_time],
            ["当前天气:", weather, "宽度(50MA):", f"{breadth:.1f}%", "VIX指数:", round(vix, 2)],
            ["视觉说明:", "🟩 强势领涨 / 🟨 短线博弈 / 🟥 高危逼空(核爆区)"]
        ]
        sh.update(values=header, range_name="A1")
        sh.format("A1:E1", {"textFormat": {"bold": True, "fontSize": 12}})
        sh.format("B2", {"backgroundColor": weather_color, "textFormat": {"bold": True}})

        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "Action", "Score", "Price", "建议买入(股)", "止损位", "EMA10乖离", "成交额(M)", "Short_SqZ", "期权异动"]
            df = df[cols]
            sh.update(values=[df.columns.tolist()] + [[robust_json_clean(c) for c in r] for r in df.values.tolist()], range_name="A5")
            
            # 2. 表头黑色背景，白色文字 (机构风格)
            sh.format("A5:J5", {
                "backgroundColor": {"red": 0.1, "green": 0.1, "blue": 0.1}, 
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}
            })

            # 3. 批量条件格式化 (一次性执行，防止 429 错误)
            formats = []
            for i, r in enumerate(res):
                row_idx = i + 6
                action = r.get("Action", "")
                short_sqz = str(r.get("Short_SqZ", ""))
                
                # A. 绿色：动量/领头羊标的 (最看好的)
                if "🚀" in action or "💎" in action:
                    formats.append({
                        "range": f"A{row_idx}:J{row_idx}",
                        "format": {"backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}} # 淡绿色
                    })
                
                # B. 黄色：反包标的 (短线机会)
                elif "⚔️" in action:
                    formats.append({
                        "range": f"A{row_idx}:J{row_idx}",
                        "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.85}} # 淡黄色
                    })

                # C. 红色：核爆区 (即使是绿色 Action，也要覆盖成红色作为警告)
                if "核爆区" in short_sqz:
                    formats.append({
                        "range": f"A{row_idx}:J{row_idx}",
                        "format": {
                            "backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}, # 淡粉红
                            "textFormat": {"foregroundColor": {"red": 0.8, "green": 0.0, "blue": 0.0}, "bold": True}
                        }
                    })
            
            if formats:
                sh.batch_format(formats)
        
        print(f"✅ 表格视觉升级成功! 时间: {bj_time}")
    except Exception as e:
        print(f"❌ 写入表格失败: {e}")
