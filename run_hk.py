# 1. 抓取基准
hsi_raw = yf.download("^HSI", period="300d", progress=False)['Close']
hsi_series = hsi_raw.iloc[:,0] if isinstance(hsi_raw, pd.DataFrame) else hsi_raw
hsi_p, hsi_ma50 = hsi_series.iloc[-1], hsi_series.rolling(50).mean().iloc[-1]

# 2. 扫描 TradingView 票池
url = "https://scanner.tradingview.com/hongkong/scan"
payload = {"columns":["name", "description", "close", "market_cap_basic", "sector"],
           "filter":[{"left": "market_cap_basic", "operation": "greater", "right": 1.2e10}],
           "range":[0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
try:
    resp = requests.post(url, json=payload, timeout=15).json().get('data',[])
    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "其他"} for d in resp])
except: return

# 3. 获取个股详情
final_list = []
tickers =[str(c).zfill(4)+".HK" for c in df_pool['code']]
data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)

for t in tickers:
    try:
        code_raw = t.split('.')[0].lstrip('0')
        if t not in data.columns.levels[0]: continue
        res = calculate_advanced_v750(data[t], hsi_series)
        if res and res['is_bull'] and res['Action'] != "观察":
            res.update({"Ticker": t.split('.')[0], "Sector": df_pool[df_pool['code']==code_raw].iloc[0]['sector']})
            final_list.append(res)
    except: continue

if not final_list: return
res_df = pd.DataFrame(final_list)

# 4. 板块配额与排名：
# 【核心重塑】废除旧分数，砍掉 70% RS_Vel，拉爆 Tight (指数级加权)
res_df['Final_Score'] = (
    res_df['Base_Score'] 
    + (res_df['RS_Vel'] * 0.6) # 被削减 70%
    + res_df['rs_raw'].rank(pct=True) * 20 
    + (10 / np.maximum(res_df['Tight'], 0.1)) ** 2  # 紧致度呈现指数级爆炸给分
).round(2)

top_picks = res_df.sort_values(by="Final_Score", ascending=False).groupby('Sector').head(4)
top_picks = top_picks.head(60) # 总榜前60

# 5. 写入与可视化
sh = init_sheet()
sh.clear()

weather = "☀️ 激进" if hsi_p > hsi_ma50 else "❄️ 观望"
header = [[f"🏰 V45-V750 量子领袖版 (极度延伸否决+暗影雷达)", f"环境: {weather}", f"刷新: {now_str}", "风控: 单笔风险 0.8% / 板块配额制"]]
sh.update(range_name="A1", values=header)

cols =["Ticker", "Action", "Final_Score", "Price", "Shares", "Stop", "Tight", "Vol_Ratio", "RS_Vel", "Dist_POC%", "PocketPivot", "ADR", "Sector"]
sh.update(range_name="A3", values=[cols] + top_picks[cols].values.tolist(), value_input_option="USER_ENTERED")

# 美化格式
set_frozen(sh, rows=3)
format_cell_range(sh, 'A3:M3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))

rules = get_conditional_format_rules(sh)
# 奇点先行 - 紫色高亮
rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
    booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['👁️']),
                            format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
# 主升浪 / 巅峰突破 - 橙红色高亮
rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
    booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🚀']),
                            format=cellFormat(backgroundColor=color(1.0, 0.9, 0.8), textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0.2, 0.2))))))
# ☠️ 极度延伸 - 灰色冷血过滤警告
rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
    booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['☠️']),
                            format=cellFormat(backgroundColor=color(0.85, 0.85, 0.85), textFormat=textFormat(bold=True, strikethrough=True, foregroundColor=color(0.5, 0.5, 0.5))))))
# 建议股数 - 绿色提醒
rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)],
    booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['0']),
                            format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0.5, 0))))))
# 🔥 Pocket Pivot 暗影雷达 - 烈焰提醒
rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('K4:K100', sh)],
    booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🔥']),
                            format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.9, 0.1, 0.1))))))
rules.save()
print(f"✅ 任务完成。成功捕捉 {len(top_picks)} 只量子领袖股。")
