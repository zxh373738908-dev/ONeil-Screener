import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from gspread_formatting import *
import datetime, time, warnings, logging, requests, os, math
import yfinance as yf

# 尝试导入美化插件
try:
    from gspread_formatting import *
    HAS_FORMATTING = True
except ImportError:
    HAS_FORMATTING = False

# 基础干扰屏蔽
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 配置中心
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))
TARGET_SHEET_NAME = "A-v7-screener" # 👈 定向目标标签页

def init_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try:
        # 尝试获取指定的 A-v7-screener
        return doc.worksheet(TARGET_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        # 如果不存在则自动创建
        return doc.add_worksheet(title=TARGET_SHEET_NAME, rows=1000, cols=20)

# ==========================================
# 🧠 2. V52.0 巅峰决策引擎 (保持统领级算法)
# ==========================================
def calculate_imperial_engine(df, index_series, mkt_cap):
    try:
        if len(df) < 200: return None
        c = df['Close'].astype(float); h = df['High'].astype(float); l = df['Low'].astype(float)
        v = df['Volume'].astype(float); o = df['Open'].astype(float)
        price = float(c.iloc[-1])
        
        # --- A. 动能加速度 (RS Accel) ---
        rs_long = (price / c.iloc[-120]) / (index_series.iloc[-1] / index_series.iloc[-120])
        rs_short = (price / c.iloc[-20]) / (index_series.iloc[-1] / index_series.iloc[-20])
        rs_accel = rs_short / (rs_long + 0.001)
        
        # --- B. 死亡寂静探测 (VDU) ---
        vdu_signal = v.iloc[-1] < (v.tail(60).mean() * 0.45)
        
        # --- C. VCP 紧致度 (最近 8 日) ---
        tightness = (h.tail(8).max() - l.tail(8).min()) / l.tail(8).min() * 100
        
        # --- D. 筹码空间演算 (Room to Run) ---
        v_hist, bins = np.histogram(c.tail(120), bins=40, weights=v.tail(120))
        curr_idx = np.searchsorted(bins, price * 1.01)
        overhead = v_hist[curr_idx:]
        target_p = bins[curr_idx + np.argmax(overhead)] if len(overhead) > 0 else price * 1.15
        room_to_run = (target_p / price - 1) * 100

        # --- E. 趋势过滤 ---
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        is_stage_2 = price > ma50 > ma200

        # --- 战术勋章判定 ---
        tag = "观察"
        if vdu_signal and tightness < 1.5 and is_stage_2:
            tag = "💀 死亡寂静(地量枢轴)"
        elif mkt_cap > 1000e8 and rs_accel > 1.1 and price > ma50:
            tag = "🛡️ 蓝筹统领(动能反转)"
        elif rs_accel > 1.3 and tightness < 2.5:
            tag = "🚀 奇点加速度(爆发)"

        score = (rs_long * 30) + (rs_accel * 30) + (room_to_run * 0.5) + (max(0, (2-tightness)*30))
        if vdu_signal: score += 15

        return {
            "action": tag, "score": score, "rs_accel": round(rs_accel, 2),
            "tight": round(tightness, 2), "room": round(room_to_run, 1),
            "vdu": "✅" if vdu_signal else "❌", "target": round(target_p, 2)
        }
    except: return None

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v52_to_v7():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V52.1 启动：目标列表 [A-v7-screener]...")

    cols = ["Ticker", "Name", "勋章", "综合评分", "动能加速度", "上涨空间%", "紧致度", "VDU地量", "行业", "现价", "目标价"]

    # 1. 基础基准
    idx_raw = yf.download("000300.SS", period="350d", progress=False)['Close'].iloc[:, 0]

    # 2. TV 云端筛选 (市值>75亿)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "close", "change"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 75e8}],
               "range": [0, 850], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])
    except: return print("❌ 接口故障")

    # 3. 执行演算
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    chunk_size = 40
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 正在演算区块 {i//chunk_size + 1}...")
        data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
        for t in chunk:
            try:
                if t not in data.columns.get_level_values(0): continue
                df_h = data[t].dropna()
                c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                
                res = calculate_imperial_engine(df_h, idx_raw, row_info['mkt'])
                if not res or res['action'] == "观察": continue

                all_hits.append({
                    "Ticker": c_code, "Name": row_info['name'], "勋章": res['action'], "综合评分": res['score'],
                    "动能加速度": res['rs_accel'], "上涨空间%": res['room'], "紧致度": res['tight'],
                    "VDU地量": res['vdu'], "行业": row_info['industry'], "现价": row_info['price'], 
                    "目标价": res['target'], "行业强度": 0
                })
            except: continue

    if not all_hits: return print("⚠️ 冰点行情，无信号")

    # 4. 🔥 板块引力加权
    res_df = pd.DataFrame(all_hits)
    industry_rs = res_df.groupby('行业')['综合评分'].transform('mean')
    res_df['行业强度'] = industry_rs
    
    def imperial_boost(row):
        final_tag = row['勋章']
        if row['行业强度'] > res_df['行业强度'].quantile(0.85):
            final_tag = f"👑 统领 | {final_tag}"
            row['综合评分'] += 15
        return final_tag

    res_df['勋章'] = res_df.apply(imperial_boost, axis=1)
    
    # 5. 排序与写入指定 Sheet
    res_df['综合评分'] = res_df['综合评分'].rank(pct=True).apply(lambda x: int(x*99))
    res_df = res_df.sort_values(by="综合评分", ascending=False).head(60)

    sh = init_sheet(); sh.clear()
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V52.1 Polaris-v7 | Target: {TARGET_SHEET_NAME} | {now_str}")

    # 6. 视觉美化
    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            # 加速度 > 1.2 标金
            rule_accel = ConditionalFormatRule(ranges=[GridRange.from_a1_range('E2:E65', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['1.2']),
                    format=cellFormat(backgroundColor=color(1, 0.9, 0.6), textFormat=textFormat(bold=True))))
            fmt_rules.append(rule_accel)
            # VDU 标红
            rule_vdu = ConditionalFormatRule(ranges=[GridRange.from_a1_range('H2:H65', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['✅']),
                    format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1, 0, 0)))))
            fmt_rules.append(rule_vdu); fmt_rules.save()
        except: pass

    print(f"✅ V52.1 任务完成！结果已送达 {TARGET_SHEET_NAME}。")

if __name__ == "__main__":
    run_v52_to_v7()
