import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
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
TARGET_SHEET_NAME = "A-v7-screener"

def init_sheet():
    # 🛡️ 解决 Exit Code 2: 预检授权文件
    if not os.path.exists(CREDS_FILE):
        print(f"❌ 致命错误: 找不到 {CREDS_FILE}。请确保已在 GitHub Secrets 中配置并生成此文件。")
        exit(1) # 强制以错误码 1 退出，方便追踪

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_key(SS_KEY)
        try:
            return doc.worksheet(TARGET_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return doc.add_worksheet(title=TARGET_SHEET_NAME, rows=1000, cols=20)
    except Exception as e:
        print(f"❌ Google Sheets 授权失败: {e}")
        exit(1)

# ==========================================
# 🧠 2. V52.2 健壮决策引擎
# ==========================================
def calculate_imperial_engine(df, index_series, mkt_cap):
    try:
        # 确保数据长度足够，防止 slicing 报错
        if len(df) < 120: return None
        
        c = df['Close'].astype(float); h = df['High'].astype(float); l = df['Low'].astype(float)
        v = df['Volume'].astype(float)
        price = float(c.iloc[-1])
        
        # --- A. 动能加速度 (RS Accel) ---
        # 增加容错：确保指数和个股长度对齐
        idx_now = index_series.iloc[-1]
        idx_prev_120 = index_series.iloc[-min(120, len(index_series))]
        idx_prev_20 = index_series.iloc[-min(20, len(index_series))]
        
        rs_long = (price / c.iloc[-min(120, len(c))]) / (idx_now / idx_prev_120)
        rs_short = (price / c.iloc[-min(20, len(c))]) / (idx_now / idx_prev_20)
        rs_accel = rs_short / (rs_long + 0.001)
        
        # --- B. 死亡寂静探测 (VDU) ---
        avg_v60 = v.tail(60).mean()
        vdu_signal = v.iloc[-1] < (avg_v60 * 0.45)
        
        # --- C. VCP 紧致度 ---
        tightness = (h.tail(8).max() - l.tail(8).min()) / (l.tail(8).min() + 0.001) * 100
        
        # --- D. 筹码空间 ---
        v_hist, bins = np.histogram(c.tail(min(120, len(c))), bins=40, weights=v.tail(min(120, len(c))))
        curr_idx = np.searchsorted(bins, price * 1.01)
        overhead = v_hist[curr_idx:]
        target_p = bins[curr_idx + np.argmax(overhead)] if len(overhead) > 0 else price * 1.15
        room_to_run = (target_p / price - 1) * 100

        # --- E. 趋势过滤 ---
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        is_stage_2 = price > ma50 > ma200

        tag = "观察"
        if vdu_signal and tightness < 1.5 and is_stage_2: tag = "💀 死亡寂静(地量)"
        elif mkt_cap > 1000e8 and rs_accel > 1.1 and price > ma50: tag = "🛡️ 蓝筹统领(加速)"
        elif rs_accel > 1.3 and tightness < 2.5: tag = "🚀 奇点爆发"

        score = (rs_long * 30) + (rs_accel * 30) + (room_to_run * 0.5) + (max(0, (2-tightness)*30))
        if vdu_signal: score += 15

        return {
            "action": tag, "score": float(score), "rs_accel": round(float(rs_accel), 2),
            "tight": round(float(tightness), 2), "room": round(float(room_to_run), 1),
            "vdu": "✅" if vdu_signal else "❌", "target": round(float(target_p), 2)
        }
    except Exception as e:
        # print(f"DEBUG: {e}")
        return None

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v52_resilient():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V52.2 启动 [A-v7-screener] 修复版...")

    cols = ["Ticker", "Name", "勋章", "综合评分", "动能加速度", "上涨空间%", "紧致度", "VDU地量", "行业", "现价", "目标价"]

    # 1. 基础基准
    try:
        idx_raw = yf.download("000300.SS", period="350d", progress=False)['Close']
        idx_series = idx_raw.iloc[:, 0] if isinstance(idx_raw, pd.DataFrame) else idx_raw
    except: return print("❌ 无法获取大盘指数")

    # 2. TV 云端筛选
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "close", "change"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 75e8}],
               "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])
    except: return print("❌ TV 接口故障")

    # 3. 执行扫描
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    chunk_size = 40
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 区块 {i//chunk_size + 1} 演算中...")
        try:
            data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
            for t in chunk:
                try:
                    # 🛡️ 解决报错: 检查 MultiIndex 数据是否存在
                    if isinstance(data.columns, pd.MultiIndex):
                        if t not in data.columns.levels[0]: continue
                        df_h = data[t].dropna()
                    else:
                        df_h = data.dropna()
                    
                    if df_h.empty: continue
                    
                    c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                    res = calculate_imperial_engine(df_h, idx_series, row_info['mkt'])
                    
                    if not res or res['action'] == "观察": continue

                    all_hits.append({
                        "Ticker": c_code, "Name": row_info['name'], "勋章": res['action'], "综合评分": res['score'],
                        "动能加速度": res['rs_accel'], "上涨空间%": res['room'], "紧致度": res['tight'],
                        "VDU地量": res['vdu'], "行业": row_info['industry'], "现价": row_info['price'], 
                        "目标价": res['target']
                    })
                except: continue
        except: continue

    # 4. 写入与兜底
    sh = init_sheet(); sh.clear()
    if not all_hits:
        sh.update_acell("A1", f"⚠️ 冰点行情，全场无 Omega 信号。 {now_str}")
        return

    res_df = pd.DataFrame(all_hits)
    res_df['综合评分'] = res_df['综合评分'].rank(pct=True).apply(lambda x: int(x*99))
    final_df = res_df.sort_values(by="综合评分", ascending=False).head(60)

    # 🛡️ 解决 KeyError: 确保所有列存在
    for col in cols:
        if col not in final_df.columns: final_df[col] = "N/A"

    sh.update(range_name="A1", values=[cols] + final_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V52.2 Origin-Resilient | SectorAlpha Sync | {now_str}")

    # 5. 格式化
    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            # 加速度 > 1.2 标金
            rule_accel = ConditionalFormatRule(ranges=[GridRange.from_a1_range('E2:E65', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['1.2']),
                    format=cellFormat(backgroundColor=color(1, 0.9, 0.6), textFormat=textFormat(bold=True))))
            fmt_rules.append(rule_accel); fmt_rules.save()
        except: pass

    print(f"🎉 V52.2 扫描圆满成功！")

if __name__ == "__main__":
    run_v52_resilient()
