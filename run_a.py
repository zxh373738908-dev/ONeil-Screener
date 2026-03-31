import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from gspread_formatting import *
import datetime, time, warnings, logging, requests, os, math
import yfinance as yf

# 基础屏蔽
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 配置中心
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))
ACCOUNT_SIZE = 100000  # 建议账户基准

def init_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try:
        return doc.worksheet("A-Share V47-Supreme")
    except:
        return doc.add_worksheet(title="A-Share V47-Supreme", rows=1000, cols=20)

# ==========================================
# 🧠 2. V47.0 统领决策引擎 (VCP + OBV + LimitUp)
# ==========================================
def calculate_v47_supreme_engine(df, index_series, mkt_cap):
    try:
        if len(df) < 200: return None
        c = df['Close']; h = df['High']; l = df['Low']; v = df['Volume']; o = df['Open']
        price = c.iloc[-1]
        
        # --- A. 趋势阶位 (Stage 2) ---
        ma50, ma200 = c.rolling(50).mean(), c.rolling(200).mean()
        is_stage_2 = price > ma50.iloc[-1] > ma200.iloc[-1]
        
        # --- B. 相对强度与 Stealth 侦测 ---
        rs_line = (c / index_series).fillna(method='ffill')
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max())
        # RS 加权分
        rs_score = (price/c.iloc[-21]*4) + (price/c.iloc[-63]*2) + (price/c.iloc[-126]) # 强化短期权重
        
        # --- C. VCP 与 机构吸筹 (OBV) ---
        tightness = (c.tail(10).std() / c.tail(10).mean()) * 100
        obv = (np.sign(c.diff()) * v).cumsum()
        is_obv_support = obv.iloc[-1] > obv.tail(10).mean() # 资金暗流向上
        
        # --- D. 涨停/大阳基因 (A股核心) ---
        # 过去 10 天是否有涨幅 > 9.5% 的 K 线
        has_limit_up = any(c.tail(10).pct_change() > 0.095)
        
        # --- E. 奇点与止损 ---
        is_rs_stealth = rs_nh and (price < c.tail(20).max() * 1.02)
        adr = ((h - l) / l).tail(20).mean()
        stop_p = price * (1 - adr * 1.8)
        suggest_shares = math.floor((ACCOUNT_SIZE * 0.015) / (price - stop_p)) if price > stop_p else 0

        # ==========================================
        # ⚔️ 指令决策树 (针对 600519 特别优化)
        # ==========================================
        action = "观察"
        # 1. 👁️ 奇点先行 (RS先行，价格未爆)
        if is_rs_stealth and tightness < 1.6 and is_obv_support:
            action = "👁️ 奇点先行(Stealth)"
        # 2. 🐉 黄金支点 (蓝筹反转买点)
        elif mkt_cap > 1000e8 and abs(price/ma50.iloc[-1]-1)<0.025 and is_obv_support:
            action = "🐉 黄金支点(Pivot)"
        # 3. 🚀 龙抬头 (涨停后的缩量回踩)
        elif has_limit_up and tightness < 2.5 and v.iloc[-1] < v.tail(5).mean():
            action = "🐲 龙回头(V-Dry)"
        # 4. ✨ 动量突破
        elif rs_nh and price >= c.tail(252).max():
            action = "🚀 动量突破(Breakout)"

        return {
            "action": action, "score": rs_score, "tight": round(tightness, 2), 
            "rs_nh": "🌟" if rs_nh else "-", "obv": "✅" if is_obv_support else "❌",
            "adr": round(adr*100, 2), "stop": round(stop_p, 2), "shares": suggest_shares,
            "has_limit": "⚡" if has_limit_up else "-"
        }
    except: return None

# ==========================================
# 🚀 3. 主扫描流程 (板块热力集成版)
# ==========================================
def run_v47_supreme():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 A股 V47.0 统领指挥部：正在执行板块军团审计...")

    # 1. 基准指数对比 (沪深300)
    idx_raw = yf.download("000300.SS", period="350d", progress=False)
    idx_close = idx_raw['Close'].iloc[:, 0] if isinstance(idx_raw['Close'], pd.DataFrame) else idx_raw['Close']
    
    # 2. TV 云端筛选 (市值>65亿)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "volume", "industry", "close"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 65e8}],
        "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([
            {"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "vol": d['d'][3], "industry": d['d'][4], "price": d['d'][5]} 
            for d in resp
        ])
    except: return print("❌ TV 接口故障")

    # 3. 演算开始
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    chunk_size = 40
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                if t not in data.columns.get_level_values(0): continue
                df_h = data[t].dropna()
                c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                
                res = calculate_v47_supreme_engine(df_h, idx_close, row_info['mkt'])
                if not res or res['action'] == "观察": continue

                all_hits.append({
                    "Ticker": c_code, "Name": row_info['name'], "指令": res['action'], 
                    "RS排名": res['score'], "行业": row_info['industry'], "板块热力": 0, # 待填
                    "OBV支撑": res['obv'], "涨停基因": res['has_limit'], "紧致度": res['tight'],
                    "ADR%": res['adr'], "建议股数": res['shares'], "现价": row_info['price']
                })
            except: continue

    if not all_hits: return print("⚠️ 暂无共振标的")

    # 4. 🔥 核心：计算板块军团浓度 (Sector Legions)
    res_df = pd.DataFrame(all_hits)
    industry_counts = res_df['行业'].value_counts()
    def supreme_boost(row):
        count = industry_counts[row['行业']]
        if count >= 3:
            row['指令'] = f"👑 统领 | {row['指令']}"
            row['RS排名'] += 10 # 板块共振加分
        row['板块热力'] = count
        return row

    res_df = res_df.apply(supreme_boost, axis=1)
    
    # 5. 排序与写入
    res_df['RS评级'] = res_df['RS排名'].rank(pct=True).apply(lambda x: int(x*99))
    res_df = res_df.sort_values(by="RS评级", ascending=False).head(60)

    sh = init_sheet(); sh.clear()
    cols = ["Ticker", "Name", "指令", "RS评级", "板块热力", "行业", "OBV支撑", "涨停基因", "紧致度", "ADR%", "建议股数", "现价"]
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("M1", f"V47.0 Supreme | Sector Legions Active | {now_str}")

    # 6. 视觉美化
    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            # 板块统领加亮
            rule_cmd = ConditionalFormatRule(
                ranges=[GridRange.from_a1_range('C2:C60', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['👑']),
                    format=cellFormat(backgroundColor=color(1, 0.9, 0.6), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0.3, 0))))
            )
            fmt_rules.append(rule_cmd); fmt_rules.save()
        except: pass

    print(f"✅ V47.0 统领系统运行完毕！")

if __name__ == "__main__":
    run_v47_supreme()
