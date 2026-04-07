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
    if not os.path.exists(CREDS_FILE):
        print(f"❌ 致命错误: 找不到 {CREDS_FILE}。")
        exit(1)

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
# 🧠 2. V52.3 增强版引擎 (针对蓝筹股优化)
# ==========================================
def calculate_imperial_engine(df, index_series, mkt_cap):
    try:
        if len(df) < 150: return None
        
        c = df['Close'].astype(float); h = df['High'].astype(float); l = df['Low'].astype(float)
        v = df['Volume'].astype(float)
        price = float(c.iloc[-1])
        
        # --- A. 相对强度 (RS) 与 加速度 ---
        idx_now = index_series.iloc[-1]
        idx_prev_120 = index_series.iloc[-120] if len(index_series) >= 120 else index_series.iloc[0]
        idx_prev_20 = index_series.iloc[-20] if len(index_series) >= 20 else index_series.iloc[0]
        
        rs_long = (price / c.iloc[-120]) / (idx_now / idx_prev_120)
        rs_short = (price / c.iloc[-20]) / (idx_now / idx_prev_20)
        rs_accel = rs_short / (rs_long + 0.001)
        
        # --- B. 紧致度与量能 ---
        avg_v60 = v.tail(60).mean()
        vdu_signal = v.iloc[-1] < (avg_v60 * 0.55) # 稍微放宽地量标准
        tightness = (h.tail(8).max() - l.tail(8).min()) / (l.tail(8).min() + 0.001) * 100
        
        # --- C. 趋势过滤 (Stage 2) ---
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        # 硬性指标：价格必须在50日线附近或上方，且50日线不破200日线
        is_stage_2 = price > ma50 * 0.98 and ma50 > ma200 * 0.95

        if not is_stage_2: return None

        # --- D. 勋章逻辑 ---
        tag = "观察"
        # 1. 奇点爆发 (高动能)
        if rs_accel > 1.25 and tightness < 3.0: 
            tag = "🚀 奇点爆发"
        # 2. 死亡寂静 (极致缩量)
        elif vdu_signal and tightness < 2.0:
            tag = "💀 死亡寂静"
        # 3. 蓝筹中流砥柱 (针对 601898 这种大市值稳健型)
        elif mkt_cap > 800e8 and rs_long > 1.1:
            tag = "🛡️ 趋势稳健(蓝筹)"
        # 4. 普通趋势优选
        elif rs_long > 1.2:
            tag = "📈 趋势走强"
        
        if tag == "观察": return None # 依然过滤掉没特点的股票

        # --- E. 筹码空间计算 ---
        v_hist, bins = np.histogram(c.tail(120), bins=30, weights=v.tail(120))
        curr_idx = np.searchsorted(bins, price)
        overhead = v_hist[curr_idx:]
        target_p = bins[curr_idx + np.argmax(overhead)] if len(overhead) > 0 else price * 1.15
        room_to_run = (target_p / price - 1) * 100

        # 综合评分：长线强度 + 加速度权重 + 紧致度奖励
        score = (rs_long * 40) + (rs_accel * 20) + (max(0, (5 - tightness) * 10))
        
        return {
            "action": tag, "score": float(score), "rs_accel": round(float(rs_accel), 2),
            "tight": round(float(tightness), 2), "room": round(float(room_to_run), 1),
            "vdu": "✅" if vdu_signal else "❌", "target": round(float(target_p), 2)
        }
    except Exception as e:
        return None

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v52_resilient():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V52.3 启动: 蓝筹+趋势增强版...")

    cols = ["Ticker", "Name", "勋章", "评分", "RS加速", "空间%", "紧致度", "VDU", "行业", "现价", "目标价"]

    # 1. 获取大盘基准
    try:
        idx_data = yf.download("000300.SS", period="350d", progress=False)['Close']
        idx_series = idx_data.iloc[:, 0] if isinstance(idx_data, pd.DataFrame) else idx_data
    except: return print("❌ 无法获取基准指数")

    # 2. 从 TradingView 筛选基础池 (市值 > 75亿)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "industry", "close"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 75e8}],
        "range": [0, 1000], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])
    except: return print("❌ TV 接口故障")

    # 3. 批量执行计算
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    chunk_size = 50
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 正在演算区块 {i//chunk_size + 1}...")
        try:
            data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
            for t in chunk:
                try:
                    # 兼容 MultiIndex 结构
                    if t not in data.columns.levels[0]: continue
                    df_h = data[t].dropna()
                    if len(df_h) < 100: continue
                    
                    c_code = t.split('.')[0]
                    row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                    
                    res = calculate_imperial_engine(df_h, idx_series, row_info['mkt'])
                    
                    if res:
                        all_hits.append({
                            "Ticker": c_code, "Name": row_info['name'], "勋章": res['action'], "评分": res['score'],
                            "RS加速": res['rs_accel'], "空间%": res['room'], "紧致度": res['tight'],
                            "VDU": res['vdu'], "行业": row_info['industry'], "现价": row_info['price'], 
                            "目标价": res['target']
                        })
                except: continue
        except: continue

    # 4. 写入 Google Sheets
    sh = init_sheet(); sh.clear()
    if not all_hits:
        sh.update_acell("A1", f"⚠️ 当前市场未扫描到符合趋势标准的个股。 {now_str}")
        return

    res_df = pd.DataFrame(all_hits)
    # 分数百分比化 (0-100分)
    if not res_df.empty:
        res_df['评分'] = res_df['评分'].rank(pct=True).apply(lambda x: int(x*99))
        final_df = res_df.sort_values(by="评分", ascending=False).head(80)
    else:
        print("❌ 无符合条件股票"); return

    # 补全缺失列
    for col in cols:
        if col not in final_df.columns: final_df[col] = "N/A"

    sh.update(range_name="A1", values=[cols] + final_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V52.3 Ultra | {now_str}")

    # 5. 自动美化格式
    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            set_column_width(sh, 'A:B', 100)
            set_column_width(sh, 'C', 150)
            # 颜色规则
            fmt_rules = get_conditional_format_rules(sh)
            rule_accel = ConditionalFormatRule(ranges=[GridRange.from_a1_range('E2:E81', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['1.1']),
                    format=cellFormat(backgroundColor=color(1, 0.9, 0.7), textFormat=textFormat(bold=True))))
            fmt_rules.append(rule_accel); fmt_rules.save()
        except: pass

    print(f"🎉 扫描完成！共发现 {len(final_df)} 只潜力股。")

if __name__ == "__main__":
    run_v52_resilient()
