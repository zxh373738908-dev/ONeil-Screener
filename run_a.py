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
ACCOUNT_SIZE = 100000 

def init_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try:
        return doc.worksheet("A-Share V50-Guardian")
    except:
        return doc.add_worksheet(title="A-Share V50-Guardian", rows=1000, cols=20)

# ==========================================
# 🧠 2. V50.0 守护者审计引擎 (大环境感知 + 枢轴触发)
# ==========================================
def calculate_guardian_engine(df, index_series, mkt_cap, market_mode):
    try:
        if len(df) < 200: return None
        c = df['Close'].astype(float); h = df['High'].astype(float); l = df['Low'].astype(float)
        v = df['Volume'].astype(float); o = df['Open'].astype(float)
        price = float(c.iloc[-1])
        
        # --- A. 均线与 52 周高位位置 ---
        ma20, ma50, ma200 = c.rolling(20).mean(), c.rolling(50).mean(), c.rolling(200).mean()
        lookback_1y = min(252, len(df))
        h52 = h.tail(lookback_1y).max()
        range_pos = (price - l.tail(lookback_1y).min()) / (h52 - l.tail(lookback_1y).min() + 0.001) * 100
        
        # --- B. 枢轴买点探测 (Pivot Point) ---
        # 寻找最近 10 天的最高价作为枢轴
        pivot_price = h.tail(10).head(9).max() 
        is_pivot_break = price >= pivot_price
        
        # --- C. RS 强度与 Stealth ---
        rs_line = (c / index_series).fillna(method='ffill')
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max())
        rs_score = (price / c.iloc[-21] * 3) + (price / c.iloc[-63] * 1) # 短期聚焦
        is_rs_stealth = rs_nh and (price < h.tail(20).max() * 1.015)

        # --- D. 机构纯度成交量 (Intensity) ---
        up_v = v[c > c.shift(1)].tail(20).mean()
        dn_v = v[c < c.shift(1)].tail(20).mean()
        vol_intensity = up_v / (dn_v + 1) # 上涨日量能比下跌日量能

        # --- E. 针对 600519 的反转探测 ---
        # 大市值 + 回踩均线止跌 + 量能背离
        is_bluechip_rebound = mkt_cap > 1000e8 and price > ma20.iloc[-1] and v.iloc[-1] < v.tail(5).mean()

        # ==========================================
        # ⚔️ 战术标签与综合评分
        # ==========================================
        action = "观察"
        # 1. 🌅 黎明枢轴 (高确定性突破点)
        if is_pivot_break and vol_intensity > 1.2 and range_pos > 65:
            action = "🌅 黎明枢轴(买点确认)"
        # 2. 🛡️ 蓝筹护盘 (600519 提早探测)
        elif is_bluechip_rebound and rs_line.iloc[-1] > rs_line.iloc[-5]:
            action = "🛡️ 蓝筹复苏(潜伏)"
        # 3. 👁️ 奇点先行
        elif is_rs_stealth and vol_intensity > 1.1:
            action = "👁️ 奇点先行(Stealth)"
        # 4. 🚀 龙抬头
        elif rs_nh and price >= h52 * 0.98:
            action = "🚀 天际突破(Breakout)"

        # 评分模型：RS(40%) + 强度(30%) + 位置(30%)
        score = (rs_score * 30) + (vol_intensity * 25) + (range_pos * 0.2)
        if market_mode == "DOWN": score *= 0.8 # 大盘不好，分值缩减
        if action != "观察": score += 20 # 战术加分

        return {
            "action": action, "score": score, "pivot": round(pivot_price, 2),
            "intensity": round(vol_intensity, 2), "rs_nh": "🌟" if rs_nh else "-",
            "r_pos": round(range_pos, 1)
        }
    except: return None

# ==========================================
# 🚀 3. 主扫描流程 (大盘制动集成版)
# ==========================================
def run_v50_guardian():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 A股 V50.0 守护者启动：正在全市场透视...")

    # 1. 🚦 大盘环境审计 (Regime Check)
    try:
        m_idx = yf.download("000300.SS", period="100d", progress=False)
        m_close = m_idx['Close'].iloc[-1].item()
        m_ma50 = m_idx['Close'].rolling(50).mean().iloc[-1]
        market_mode = "UP" if m_close > m_ma50 else "DOWN"
        print(f" -> 🚦 当前大盘模式: {market_mode}")
    except: market_mode = "UP"

    # 2. TV 云端筛选 (市值>65亿)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "close", "change"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 65e8}],
               "range": [0, 850], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4], "chg": d['d'][5]} for d in resp])
    except: return print("❌ 接口断开")

    # 3. 核心扫描
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    chunk_size = 40
    idx_raw = yf.download("000300.SS", period="350d", progress=False)['Close'].iloc[:, 0]
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
        for t in chunk:
            try:
                if t not in data.columns.get_level_values(0): continue
                df_h = data[t].dropna()
                c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                
                res = calculate_guardian_engine(df_h, idx_raw, row_info['mkt'], market_mode)
                if not res or res['action'] == "观察": continue

                all_hits.append({
                    "Ticker": c_code, "Name": row_info['name'], "指令": res['action'], "枢轴买点": res['pivot'],
                    "综合评分": res['score'], "52W位置%": res['r_pos'], "量能强度": res['intensity'],
                    "行业": row_info['industry'], "现价": row_info['price'], "板块热力": 0
                })
            except: continue

    if not all_hits: return print("⚠️ 行情极寒，无幸存火种")

    # 4. 🔥 军团共振逻辑
    res_df = pd.DataFrame(all_hits)
    industry_counts = res_df['行业'].value_counts()
    def guardian_boost(row):
        count = industry_counts[row['行业']]
        if count >= 3:
            row['指令'] = f"🛡️ 军团 | {row['指令']}"
            row['综合评分'] += 15
        row['板块热力'] = count
        return row
    res_df = res_df.apply(guardian_boost, axis=1)
    
    # 5. 排序与写入
    res_df['RS评级'] = res_df['综合评分'].rank(pct=True).apply(lambda x: int(x*99))
    res_df = res_df.sort_values(by="RS评级", ascending=False).head(60)

    sh = init_sheet(); sh.clear()
    cols = ["Ticker", "Name", "指令", "RS评级", "枢轴买点", "52W位置%", "量能强度", "板块热力", "行业", "现价"]
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V50.0 Guardian | 大盘水位: {market_mode} | {now_str}")

    # 6. 视觉美化
    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            # 枢轴点买点加黄
            rule_pivot = ConditionalFormatRule(ranges=[GridRange.from_a1_range('E2:E65', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['0']),
                    format=cellFormat(backgroundColor=color(1, 1, 0.8), textFormat=textFormat(bold=True))))
            fmt_rules.append(rule_pivot); fmt_rules.save()
        except: pass

    print(f"✅ V50.0 守护者任务完成！")

if __name__ == "__main__":
    run_v50_guardian()
