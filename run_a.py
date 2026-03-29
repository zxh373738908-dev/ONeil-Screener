import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
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
        return doc.worksheet("A-Share V37-Omniscience")
    except:
        return doc.add_worksheet(title="A-Share V37-Omniscience", rows=1000, cols=20)

# ==========================================
# 🧠 2. V37.0 “全知者”核心算法
# ==========================================
def analyze_v37_omniscience(df, rs_val, mkt_cap):
    try:
        sub_df = df.tail(252)
        c = sub_df['Close'].values; h = sub_df['High'].values; l = sub_df['Low'].values
        v = sub_df['Volume'].values; o = sub_df['Open'].values
        price = c[-1]
        
        # --- A. 均线系统 (MA50/200) ---
        ma50 = sub_df['Close'].rolling(50).mean().iloc[-1]
        ma200 = sub_df['Close'].rolling(200).mean().iloc[-1]
        dist_ma50 = (price / ma50 - 1) * 100
        dist_ma200 = (price / ma200 - 1) * 100
        
        # --- B. 机构护盘止跌探测 ---
        # 逻辑：过去5天内有缩量(VDU)，且今日收盘高于昨日高点(吞噬/突破)
        vdu_5d = np.min(v[-5:]) < (np.mean(v[-60:]) * 0.5)
        is_reversal = price > h[-2] and v[-1] > v[-2]
        
        # --- C. 52周位置 ---
        h52 = np.max(h); l52 = np.min(l)
        range_pos = (price - l52) / (h52 - l52) * 100
        
        # --- D. 筹码与空间 ---
        v_hist, bins = np.histogram(c[-120:], bins=50, weights=v[-120:])
        curr_bin_idx = np.searchsorted(bins, price)
        overhead_bins = v_hist[curr_bin_idx:]
        target_p = bins[curr_bin_idx + np.argmax(overhead_bins)] if len(overhead_bins) > 0 else price * 1.15
        
        tr = pd.concat([sub_df['High']-sub_df['Low'], abs(sub_df['High']-sub_df['Close'].shift())], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        stop_p = price - (atr * 1.5)
        rr_ratio = (target_p - price) / (price - stop_p) if (price - stop_p) > 0 else 0

        # ==========================================
        # ⚔️ 核心战法分队 (含茅台类蓝筹反弹)
        # ==========================================
        tag = "持有"
        # 1. 🛡️ 龙之支点 (专门针对 600519 这种大盘白马反弹)
        if mkt_cap > 800e8 and abs(dist_ma50) < 3.5 and is_reversal:
            tag = "🛡️龙之支点(蓝筹反弹)"
        # 2. 💎 极光核心 (右侧主升浪)
        elif range_pos > 75 and rr_ratio > 2.5 and vdu_5d:
            tag = "💎极光核心(主升)"
        # 3. 🗡️ 执剑枢轴
        elif vdu_5d and price > ma50:
            tag = "🗡️执剑枢轴"
        
        if range_pos < 40: tag = "🔍观察期"

        # --- E. 评分逻辑 (对蓝筹支点加分) ---
        score = (rs_val * 35) + (25 if tag == "🛡️龙之支点(蓝筹反弹)" else 0) + (min(rr_ratio, 5) * 10)
        if range_pos > 70: score += 15

        return tag, round(score, 1), round(target_p, 2), round(stop_p, 2), round(range_pos, 1), round(dist_ma50, 1)
    except:
        return "ERR", 0, 0, 0, 0, 0

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v37_omniscience():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V37.0 Omniscience 启动 (全视角: 包含蓝筹支点探测)...")

    # 1. 基础基准
    try:
        idx = yf.download("000300.SS", period="350d", progress=False)
        idx_c = idx['Close'].iloc[:, 0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close']
    except: return print("❌ 基准指数下载失败")

    # 2. TV 云端筛选 (稍微调低门槛以捕捉反弹初期的标的)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "volume", "close", "industry"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 50e8}], # 50亿以上
        "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][5]} for d in raw_data])
    except: return print("❌ 接口异常")

    # 3. 扫描
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    final_list = []
    chunk_size = 60
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 全知扫描进度: {i+1} ~ {min(i+chunk_size, len(tickers))}...")
        data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                df_h = data[t].dropna()
                if len(df_h) < 252: continue
                p = df_h['Close'].iloc[-1]
                rs_raw = (p / df_h['Close'].iloc[-120]) / (idx_c.iloc[-1] / idx_c.iloc[-120])
                
                c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                
                tag, score, target, stop, r_pos, d_ma50 = analyze_v37_omniscience(df_h, rs_raw, row_info['mkt'])
                
                if score < 35: continue

                final_list.append({
                    "Ticker": c_code, "Name": row_info['name'], "评分": score, "战术勋章": tag, 
                    "距MA50%": d_ma50, "52周位置%": r_pos, "目标价": target, "止损价": stop,
                    "市值(亿)": round(row_info['mkt']/1e8, 2), "行业": row_info['industry'], "RS强度": round(rs_raw, 2)
                })
            except: continue

    if not final_list: return
    res_df = pd.DataFrame(final_list).sort_values(by="评分", ascending=False).head(60)

    # 4. 写入
    sh = init_sheet(); sh.clear()
    cols = ["Ticker", "Name", "评分", "战术勋章", "距MA50%", "52周位置%", "目标价", "止损价", "市值(亿)", "行业", "RS强度"]
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V37.0 Omniscience Active | 🛡️DragonPivot Mode ON | {now_str}")

    print(f"✅ V37.0 扫描大功告成！已锁定蓝筹支点标的。")

if __name__ == "__main__":
    run_v37_omniscience()
