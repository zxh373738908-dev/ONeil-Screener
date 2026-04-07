import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
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
    """初始化 Google Sheets 链接，确保流程不中断"""
    if not os.path.exists(CREDS_FILE):
        print(f"❌ 致命错误: 找不到 {CREDS_FILE}。请确保已在 Secrets 中配置。")
        exit(1)

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_key(SS_KEY)
        try:
            return doc.worksheet(TARGET_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            print(f"⚠️ 标签页 {TARGET_SHEET_NAME} 不存在，正在创建...")
            return doc.add_worksheet(title=TARGET_SHEET_NAME, rows=1000, cols=20)
    except Exception as e:
        print(f"❌ Google Sheets 授权或访问失败: {e}")
        exit(1)

# ==========================================
# 🧠 2. V52.3 增强版决策引擎
# ==========================================
def calculate_imperial_engine(df, index_series, mkt_cap):
    try:
        # 数据长度预检 (至少需要 120 日数据)
        if len(df) < 120: return None
        
        c = df['Close'].astype(float)
        h = df['High'].astype(float)
        l = df['Low'].astype(float)
        v = df['Volume'].astype(float)
        price = float(c.iloc[-1])
        
        # --- A. 相对强度 (Relative Strength) ---
        idx_now = index_series.iloc[-1]
        idx_prev_120 = index_series.iloc[-120] if len(index_series) >= 120 else index_series.iloc[0]
        idx_prev_20 = index_series.iloc[-20] if len(index_series) >= 20 else index_series.iloc[0]
        
        # 长线 RS (半年期) 与 短线 RS (月期)
        rs_long = (price / c.iloc[-120]) / (idx_now / idx_prev_120)
        rs_short = (price / c.iloc[-20]) / (idx_now / idx_prev_20)
        rs_accel = rs_short / (rs_long + 0.001) # 加速度
        
        # --- B. 波动与量能 ---
        avg_v60 = v.tail(60).mean()
        vdu_signal = v.iloc[-1] < (avg_v60 * 0.50) # 修正：50%日均量即认定为缩量
        # 8日价格紧致度 (振幅)
        tightness = (h.tail(8).max() - l.tail(8).min()) / (l.tail(8).min() + 0.001) * 100
        
        # --- C. 趋势过滤 (Stage 2 改良版) ---
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        # 允许股价在 50日线下方 2% 以内（针对回踩确认的股票，如中煤能源）
        is_stage_2 = price > ma50 * 0.98 and ma50 > ma200 * 0.95

        if not is_stage_2: return None

        # --- D. 勋章逻辑分类 (解决选不出 601898 的关键) ---
        tag = "观察"
        if rs_accel > 1.25 and tightness < 3.0: 
            tag = "🚀 奇点爆发"
        elif vdu_signal and tightness < 2.0:
            tag = "💀 死亡寂静"
        elif mkt_cap > 800e8 and rs_long > 1.05:
            # 针对大市值蓝筹：长线走势强于大盘即入选
            tag = "🛡️ 蓝筹趋势"
        elif rs_long > 1.15:
            tag = "📈 趋势走强"
        
        if tag == "观察": return None 

        # --- E. 空间计算 ---
        v_hist, bins = np.histogram(c.tail(120), bins=30, weights=v.tail(120))
        curr_idx = np.searchsorted(bins, price)
        overhead = v_hist[curr_idx:]
        target_p = bins[curr_idx + np.argmax(overhead)] if len(overhead) > 0 else price * 1.15
        room_to_run = (target_p / price - 1) * 100

        # 评分公式：长线RS权重40% + 加速度权重30% + 紧致度奖励
        score = (rs_long * 40) + (rs_accel * 30) + (max(0, (4 - tightness) * 15))
        
        return {
            "action": tag, "score": float(score), "rs_accel": round(float(rs_accel), 2),
            "tight": round(float(tightness), 2), "room": round(float(room_to_run), 1),
            "vdu": "✅" if vdu_signal else "❌", "target": round(float(target_p), 2)
        }
    except Exception:
        return None

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v52_optimized():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V52.3 启动 | 深度扫描 A 股池...")

    # 1. 基准指数 (沪深300)
    try:
        idx_raw = yf.download("000300.SS", period="350d", progress=False)['Close']
        idx_series = idx_raw.iloc[:, 0] if isinstance(idx_raw, pd.DataFrame) else idx_raw
    except: return print("❌ 无法获取指数数据")

    # 2. TV 基础池
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "industry", "close"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}],
        "range": [0, 1000], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])
    except: return print("❌ TV 接口访问失败")

    # 3. 循环计算
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    chunk_size = 50
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 处理区块 {i//chunk_size + 1} ({len(chunk)} 只)...")
        try:
            data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
            for t in chunk:
                try:
                    # 🛡️ 稳健的 Dataframe 提取逻辑
                    if isinstance(data.columns, pd.MultiIndex):
                        if t not in data.columns.levels[0]: continue
                        df_h = data[t].dropna()
                    else:
                        df_h = data.dropna()
                    
                    if len(df_h) < 100: continue
                    
                    c_code = t.split('.')[0]
                    row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                    res = calculate_imperial_engine(df_h, idx_series, row_info['mkt'])
                    
                    if res:
                        all_hits.append({
                            "代码": c_code, "名称": row_info['name'], "勋章": res['action'], 
                            "综合评分": res['score'], "RS加速": res['rs_accel'], 
                            "空间%": res['room'], "紧致度": res['tight'], "VDU": res['vdu'],
                            "行业": row_info['industry'], "现价": row_info['price'], "目标价": res['target']
                        })
                except: continue
        except: continue

    # 4. 排名与写入
    sh = init_sheet(); sh.clear()
    if not all_hits:
        sh.update_acell("A1", f"⚠️ 市场弱势，无符合趋势信号。 {now_str}")
        return

    res_df = pd.DataFrame(all_hits)
    res_df['综合评分'] = res_df['综合评分'].rank(pct=True).apply(lambda x: int(x*99))
    final_df = res_df.sort_values(by="综合评分", ascending=False).head(70)

    cols = ["代码", "名称", "勋章", "综合评分", "RS加速", "空间%", "紧致度", "VDU", "行业", "现价", "目标价"]
    sh.update(range_name="A1", values=[cols] + final_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V52.3 Final | {now_str}")

    # 5. 条件格式
    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            # 加速度高亮 (E列)
            rule_accel = ConditionalFormatRule(ranges=[GridRange.from_a1_range('E2:E71', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['1.15']),
                    format=cellFormat(backgroundColor=color(1, 0.9, 0.7), textFormat=textFormat(bold=True))))
            fmt_rules.append(rule_accel); fmt_rules.save()
        except: pass

    print(f"🎉 扫描圆满完成，已更新至 Google Sheets！")

if __name__ == "__main__":
    run_v52_optimized()
