import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from gspread_formatting import *
import datetime, time, warnings, logging, requests
import yfinance as yf

# 屏蔽干扰
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 配置中心
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try:
        return doc.worksheet("A-Share V23-Aegis")
    except:
        return doc.add_worksheet(title="A-Share V23-Aegis", rows=1000, cols=20)

# ==========================================
# 🧠 2. V23.0 “神盾”演算引擎
# ==========================================
def analyze_v23_logic(df, rs_raw_val, sector_rank_bonus, breadth_factor):
    try:
        c = df['Close'].values; h = df['High'].values; l = df['Low'].values; v = df['Volume'].values; o = df['Open'].values
        price = c[-1]
        
        # --- A. 机构口袋买点 (Pocket Pivot) ---
        # 逻辑：上涨，且今日量 > 过去10天内最大阴线量
        rets = np.diff(c[-11:]) / c[-12:-1]
        vols_10 = v[-11:-1]
        max_down_vol = max([vols_10[i] for i in range(10) if rets[i] < 0] or [0])
        is_pocket_pivot = price > o[-1] and v[-1] > max_down_vol
        
        # --- B. VCP 阶梯收缩率 (延续 V22) ---
        v_now = np.std((h[-10:] - l[-10:]) / l[-10:] * 100)
        v_prev = np.std((h[-20:-10] - l[-20:-10]) / l[-20:-10] * 100)
        vcp_ratio = v_now / v_prev if v_prev > 0 else 1.0
        
        # --- C. 缺口与形体 ---
        gap_up = (o[-1] / c[-2] - 1) * 100
        rhc = (price - l[-1]) / (h[-1] - l[-1]) if (h[-1] - l[-1]) > 0 else 0.5
        ma20 = df['Close'].rolling(20).mean().iloc[-1]
        ext = (price / ma20 - 1) * 100
        
        # --- D. 筹码与止损 ---
        v_profile, bins = np.histogram(c[-120:], bins=50, weights=v[-120:])
        poc = (bins[np.argmax(v_profile)] + bins[np.argmax(v_profile)+1]) / 2
        tr = pd.concat([df['High']-df['Low'], abs(df['High']-df['Close'].shift()), abs(df['Low']-df['Close'].shift())], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        stop_loss = price - (atr * 1.5)
        
        # --- E. 战术标签 ---
        tag = ""
        if is_pocket_pivot and vcp_ratio < 0.8: tag = "💎口袋起爆"
        elif gap_up > 2.5 and rhc > 0.8: tag = "⚡️强力缺口"
        elif abs(price-poc)/poc < 0.03: tag = "🐉老龙回头"
        else: tag = "📈趋势中"

        # --- F. V23 终极综合评分 ---
        # RS(50%) + 板块红利(15%) + 机构口袋(15%) + VCP收缩(20%) * 广度调节
        score = (rs_raw_val * 50) + sector_rank_bonus
        if is_pocket_pivot: score += 15
        if vcp_ratio < 0.7: score += 15
        if gap_up > 3: score += 10
        score *= breadth_factor

        return tag, poc, "✅" if is_pocket_pivot else "❌", round(vcp_ratio, 2), round(stop_loss, 2), round(score, 1), round(ext, 1)
    except:
        return "ERR", 0, "❌", 0, 0, 0, 0

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v23_aegis():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V23.0 Aegis 启动 (板块Alpha+机构口袋买点)...")

    # 1. 广度与基准
    idx = yf.download("000300.SS", period="350d", progress=False)
    idx_c = idx['Close'].iloc[:, 0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close']

    # 2. TV 云端名单 (市值>80亿)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "volume", "close", "industry", "change"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 8e9}],
        "range": [0, 600], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "industry": d['d'][5], "chg": d['d'][6], "mkt": d['d'][2]} for d in raw_data])

    # --- 3. 计算板块Alpha (Sector Leadership) ---
    print(" -> 📡 计算板块热力图 (Sector Alpha)...")
    sector_performance = df_pool.groupby('industry')['chg'].mean().sort_values(ascending=False)
    # 给排名前 20% 的板块加 20 分红利
    top_sectors = sector_performance.head(len(sector_performance)//5).index.tolist()

    # --- 4. 广度探测 ---
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    breadth_check = yf.download(tickers[:100], period="250d", progress=False)['Close']
    uptrend_count = sum([1 for t in breadth_check.columns if breadth_check[t].iloc[-1] > breadth_check[t].rolling(200).mean().iloc[-1]])
    breadth_factor = max(0.5, min(1.0, (uptrend_count / 100) * 1.6))

    # --- 5. 正式扫描 ---
    final_list = []
    chunk_size = 50
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
        for t in chunk:
            try:
                df_h = data[t].dropna()
                if len(df_h) < 200: continue
                p = df_h['Close'].iloc[-1]
                rs_raw = (p / df_h['Close'].iloc[-250]) / (idx_c.iloc[-1] / idx_c.iloc[-250])
                if rs_raw < 1.05: continue

                c_code = t.split('.')[0]
                row = df_pool[df_pool['code'] == c_code].iloc[0]
                bonus = 20 if row['industry'] in top_sectors else 0
                
                tag, poc, pocket, vcp_r, stop, score, ext = analyze_v23_logic(df_h, rs_raw, bonus, breadth_factor)
                
                if score < 55: continue

                final_list.append({
                    "Ticker": c_code, "Name": row['name'], "综合评分": score, "战术勋章": tag, 
                    "机构口袋": pocket, "行业": row['industry'], "止损点": stop, "VCP收缩率": vcp_r, "MA20偏离": ext, "RS评级": rs_raw
                })
            except: continue

    if not final_list: return

    # 6. 排序与写入
    res_df = pd.DataFrame(final_list)
    res_df['RS评级'] = res_df['RS评级'].rank(pct=True).apply(lambda x: int(x*99))
    res_df = res_df.sort_values(by="综合评分", ascending=False).head(60)

    sh = init_sheet(); sh.clear()
    cols = ["Ticker", "Name", "综合评分", "战术勋章", "机构口袋", "行业", "止损点", "VCP收缩率", "MA20偏离", "RS评级"]
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("K1", f"V23.0 Aegis Mode | Breadth: {round(breadth_factor,2)} | Updated: {now_str}")

    # 7. 视觉美化 (口袋买点与行业龙头)
    set_frozen(sh, rows=1)
    fmt_rules = get_conditional_format_rules(sh)
    # 口袋买点 ✅ 加亮
    rule_pocket = ConditionalFormatRule(ranges=[GridRange.from_a1_range('E2:E60', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['✅']),
            format=cellFormat(backgroundColor=color(0.8, 1, 0.8), textFormat=textFormat(bold=True))))
    fmt_rules.append(rule_pocket)
    fmt_rules.save()
    
    print(f"✅ V23.0 Aegis 部署完成！锁定真龙。")

if __name__ == "__main__":
    run_v23_aegis()
