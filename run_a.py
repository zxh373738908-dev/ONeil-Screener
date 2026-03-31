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
        return doc.worksheet("A-Share V49-Galaxy")
    except:
        return doc.add_worksheet(title="A-Share V49-Galaxy", rows=1000, cols=20)

# ==========================================
# 🧠 2. V49.0 统领审计引擎 (RHC + Sector Pulse + Spring)
# ==========================================
def calculate_galaxy_engine(df, index_series, mkt_cap):
    try:
        if len(df) < 150: return None
        # 数据净化，防止 ValueError
        c = df['Close'].astype(float); h = df['High'].astype(float); l = df['Low'].astype(float)
        v = df['Volume'].astype(float); o = df['Open'].astype(float)
        price = float(c.iloc[-1])
        
        # --- A. 趋势模板与弹簧容错 ---
        ma50, ma150, ma200 = c.rolling(50).mean(), c.rolling(150).mean(), c.rolling(200).mean()
        # 标准 Stage 2 或 针对大票的 Spring (回踩 MA50 企稳)
        is_stage_2 = price > ma50.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1]
        is_bluechip_spring = mkt_cap > 1000e8 and abs(price/ma50.iloc[-1]-1) < 0.03 and price > o.iloc[-1]
        
        # --- B. 收盘位置强度 (RHC) - A股防骗炮核心 ---
        rhc = (price - l.iloc[-1]) / (h.iloc[-1] - l.iloc[-1] + 0.001)
        
        # --- C. 相对强度与 Stealth (奇点先行) ---
        rs_line = (c / index_series).fillna(method='ffill')
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max())
        rs_score = (price / c.iloc[-min(21, len(c))] * 4) + (price / c.iloc[-min(63, len(c))] * 2) # 短期加权
        
        is_rs_stealth = rs_nh and (price < c.tail(20).max() * 1.015)
        
        # --- D. 机构吸筹 (U/D & OBV) ---
        up_vol = v[c > c.shift(1)].tail(50).sum()
        dn_vol = v[c < c.shift(1)].tail(50).sum()
        ud_ratio = up_vol / (dn_vol + 1)
        obv_up = v.iloc[-1] > v.tail(10).mean() and price > o.iloc[-1] # 今日放量收阳
        
        # --- E. ADR 动态止损 ---
        adr = ((h - l) / l).tail(20).mean()
        stop_p = price * (1 - adr * 1.8)

        # ==========================================
        # ⚔️ 决策勋章
        # ==========================================
        action = "观察"
        # 1. 👁️ 奇点先行 (针对 600519 这种大盘股初动)
        if is_rs_stealth and rhc > 0.6:
            action = "👁️ 奇点先行(Stealth)"
        # 2. 🐉 黄金支点 (白马反转)
        elif is_bluechip_spring and ud_ratio > 1.1:
            action = "🐉 黄金支点(Pivot)"
        # 3. 🚀 动量爆发 (主升浪)
        elif rs_nh and price >= c.tail(120).max() * 0.98 and rhc > 0.8:
            action = "🚀 动量爆发(Breakout)"
        # 4. ✨ 极速紧缩 (VCP)
        elif is_stage_2 and (h.tail(5).max() - l.tail(5).min()) / l.tail(5).min() < 0.04:
            action = "✨ 极速紧缩(VCP)"

        return {
            "action": action, "rs_raw": rs_score, "rhc": round(rhc, 2), 
            "ud": round(ud_ratio, 2), "rs_nh": "🌟" if rs_nh else "-",
            "stop": round(stop_p, 2), "adr": round(adr*100, 2), "obv_up": obv_up
        }
    except: return None

# ==========================================
# 🚀 3. 主扫描流程 (板块军团集成)
# ==========================================
def run_v49_galaxy():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 A股 V49.0 银河·统领启动：正在执行军团级审计...")

    # 1. 基准
    idx_raw = yf.download("000300.SS", period="300d", progress=False)
    idx_close = idx_raw['Close'].iloc[:, 0] if isinstance(idx_raw['Close'], pd.DataFrame) else idx_raw['Close']
    
    # 2. TV 筛选 (市值 > 65 亿)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "volume", "industry", "close", "change"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 65e8}],
        "range": [0, 850], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([
            {"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][4], "price": d['d'][5], "chg": d['d'][6]} 
            for d in resp
        ])
    except: return print("❌ TV 接口故障")

    # 3. 扫描个股
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    chunk_size = 40
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 扫描进度: {i+1} ~ {min(i+chunk_size, len(tickers))}...")
        data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                if t not in data.columns.get_level_values(0): continue
                df_h = data[t].dropna()
                c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                
                res = calculate_galaxy_engine(df_h, idx_close, row_info['mkt'])
                if not res or res['action'] == "观察": continue

                all_hits.append({
                    "Ticker": c_code, "Name": row_info['name'], "指令": res['action'], 
                    "RS排名": res['rs_raw'], "收盘强度": res['rhc'], "U/D比": res['ud'],
                    "ADR%": res['adr'], "止损价": res['stop'], "行业": row_info['industry'], 
                    "现价": row_info['price'], "板块热力": 0
                })
            except: continue

    if not all_hits: return print("⚠️ 暂无共振标的")

    # 4. 🔥 军团共振逻辑 (Galaxy Sector Pulse)
    res_df = pd.DataFrame(all_hits)
    industry_counts = res_df['行业'].value_counts()
    
    def galaxy_boost(row):
        count = industry_counts[row['行业']]
        if count >= 3:
            row['指令'] = f"👑 统领 | {row['指令']}"
            row['RS排名'] *= 1.2 # 板块加成
        row['板块热力'] = count
        return row

    res_df = res_df.apply(galaxy_boost, axis=1)
    
    # 5. 排序与写入
    res_df['RS评级'] = res_df['RS排名'].rank(pct=True).apply(lambda x: int(x*99))
    res_df = res_df.sort_values(by="RS评级", ascending=False).head(60)

    sh = init_sheet(); sh.clear()
    cols = ["Ticker", "Name", "指令", "RS评级", "板块热力", "收盘强度", "U/D比", "ADR%", "止损价", "行业", "现价"]
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V49.0 Galaxy-Commander | RHC Filter Active | {now_str}")

    # 6. 视觉美化
    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            # 统领级军团标金
            rule_cmd = ConditionalFormatRule(
                ranges=[GridRange.from_a1_range('C2:C65', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['👑']),
                    format=cellFormat(backgroundColor=color(1, 0.9, 0.6), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0.2, 0))))
            )
            fmt_rules.append(rule_cmd); fmt_rules.save()
        except: pass

    print(f"✅ V49.0 银河·统领审计大功告成！")

if __name__ == "__main__":
    run_v49_galaxy()
