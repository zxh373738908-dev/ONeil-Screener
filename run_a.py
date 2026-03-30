import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
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

def init_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try:
        return doc.worksheet("A-Share V42-Imperial")
    except:
        return doc.add_worksheet(title="A-Share V42-Imperial", rows=1000, cols=20)

# ==========================================
# 🧠 2. V42.0 “帝星”决策演算引擎
# ==========================================
def analyze_v42_logic(df, mkt_cap, sector_alpha):
    try:
        sub_df = df.tail(252).copy()
        c = sub_df['Close'].values; h = sub_df['High'].values; l = sub_df['Low'].values
        v = sub_df['Volume'].values; o = sub_df['Open'].values
        price = c[-1]
        
        # --- A. 机构口袋买点 (Pocket Pivot) ---
        # 逻辑：今日上涨，且量 > 过去10天内最大阴线量 (Minervini/O'Neil 核心)
        rets_10 = np.diff(c[-11:]) / c[-12:-1]
        vols_10 = v[-11:-1]
        max_down_vol = max([vols_10[i] for i in range(10) if rets_10[i] < 0] or [0.1])
        is_pocket = price > o[-1] and v[-1] > max_down_vol
        
        # --- B. VCP 紧致度指数 (Volatility Ratio) ---
        v_std_10 = np.std((h[-10:] - l[-10:]) / l[-10:] * 100)
        v_std_50 = np.std((h[-50:] - l[-50:]) / l[-50:] * 100)
        vcp_idx = v_std_10 / v_std_50 if v_std_50 > 0 else 1.0
        
        # --- C. 均线与 52 周位置 ---
        ma50 = sub_df['Close'].rolling(50).mean().iloc[-1]
        ma200 = sub_df['Close'].rolling(200).mean().iloc[-1]
        h52 = np.max(h); l52 = np.min(l)
        range_pos = (price - l52) / (h52 - l52) * 100

        # --- D. 筹码分布 (POC) 与空间 ---
        v_hist, bins = np.histogram(c[-120:], bins=40, weights=v[-120:])
        curr_idx = np.searchsorted(bins, price * 1.01)
        overhead = v_hist[curr_idx:]
        target_p = bins[curr_idx + np.argmax(overhead)] if len(overhead) > 0 else price * 1.15
        profit_room = (target_p / price - 1) * 100

        # ==========================================
        # ⚔️ 战法分队勋章 (锁定茅台反转)
        # ==========================================
        tag = "持有"
        # 1. 🏆 帝星反转 (锁定 600519/大蓝筹底)
        if mkt_cap > 1000e8 and is_pocket and (price > ma50 or vcp_idx < 0.6):
            tag = "🏆帝星反转(机构重仓)"
        # 2. 💎 黄金枢轴 (VCP 突破)
        elif is_pocket and vcp_idx < 0.5 and range_pos > 70:
            tag = "💎黄金枢轴(洗盘结束)"
        # 3. 🚀 能量喷发 (主升加速)
        elif rs_val > 1.2 and v[-1] > np.mean(v[-50:]) * 2 and price > ma50:
            tag = "🚀能量喷发(加速)"
        
        if range_pos < 40: tag = "🔍筑底期"

        # --- E. 综合评分 (加上板块 Alpha 与 VCP 指数) ---
        score = (rs_val * 35) + (sector_alpha * 15) + (25 if is_pocket else 0) + (max(0, (1-vcp_idx)*30))
        if tag == "🏆帝星反转(机构重仓)": score += 20

        return tag, round(score, 1), round(vcp_idx, 2), round(target_p, 2), round(profit_room, 1), range_pos
    except:
        return "ERR", 0, 0, 0, 0, 0

# ==========================================
# 🚀 3. 主扫描流程 (统领版)
# ==========================================
def run_v42_imperial():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V42.0 Imperial 启动 (帝星反转+RS百分比评级)...")

    cols = ["Ticker", "Name", "综合评分", "RS评级", "战术勋章", "上涨空间%", "VCP指数", "行业", "市值(亿)", "Price"]

    # 1. TV 筛选 (放宽门槛以捕捉白马底)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "change"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 65e8}],
               "range": [0, 850], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    
    try:
        raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([
            {"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "chg": d['d'][4]} 
            for d in raw_data
        ])
        sector_alpha = df_pool.groupby('industry')['chg'].mean().to_dict()
    except: return print("❌ 接口故障")

    # 2. 基准
    idx = yf.download("000300.SS", period="300d", progress=False)
    idx_c = idx['Close'].iloc[:, 0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close']

    # 3. 扫描演算
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    all_hits = []
    chunk_size = 25
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 帝星演算区块 {i//chunk_size + 1}...")
        try:
            data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True, timeout=10)
            for t in chunk:
                try:
                    if t not in data.columns.get_level_values(0): continue
                    df_h = data[t].dropna()
                    if len(df_h) < 150: continue
                    
                    p = df_h['Close'].iloc[-1]
                    global rs_val # 临时全局用于评分函数
                    rs_val = (p / df_h['Close'].iloc[-120]) / (idx_c.iloc[-1] / idx_c.iloc[-120])
                    
                    c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                    s_alpha = sector_alpha.get(row_info['industry'], 0)
                    
                    tag, score, vcp, target, room, r_pos = analyze_v42_logic(df_h, row_info['mkt'], s_alpha)
                    
                    if score < 30: continue

                    all_hits.append({
                        "Ticker": c_code, "Name": row_info['name'], "综合评分": score, "战术勋章": tag, 
                        "上涨空间%": room, "VCP指数": vcp, "行业": row_info['industry'], 
                        "RS强度": rs_val, "市值(亿)": round(row_info['mkt']/1e8, 2), "Price": round(float(p), 2)
                    })
                except: continue
        except: continue

    if not all_hits: return print("⚠️ 无信号")

    # 4. 🏆 计算 RS 百分比评级 (V42 核心)
    res_df = pd.DataFrame(all_hits)
    res_df['RS评级'] = res_df['RS强度'].rank(pct=True).apply(lambda x: int(x*99))
    
    # 5. 写入
    res_df = res_df.sort_values(by="综合评分", ascending=False).head(60)
    sh = init_sheet(); sh.clear()
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V42.0 Imperial | 👑RS Percentile Active | SectorAlpha Sync | {now_str}")

    print(f"✅ V42.0 帝星任务完成！")

if __name__ == "__main__":
    run_v42_imperial()
