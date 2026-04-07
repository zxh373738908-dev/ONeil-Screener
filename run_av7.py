import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
import yfinance as yf

# 环境屏蔽
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
        print(f"❌ 错误: 找不到 {CREDS_FILE}"); exit(1)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_key(SS_KEY)
        return doc.worksheet(TARGET_SHEET_NAME) if TARGET_SHEET_NAME in [w.title for w in doc.worksheets()] else doc.add_worksheet(TARGET_SHEET_NAME, 1000, 20)
    except Exception as e: print(f"❌ 授权失败: {e}"); exit(1)

# ==========================================
# 🧠 2. V52.7 Super Performance 核心引擎
# ==========================================
def calculate_vcp_engine(df, idx_df, mkt_cap):
    try:
        if len(df) < 250: return None
        
        c = df['Close'].astype(float); h = df['High'].astype(float)
        l = df['Low'].astype(float); v = df['Volume'].astype(float)
        price = float(c.iloc[-1])
        
        # 0. 流动性与基础趋势过滤
        if (c * v).tail(20).mean() < 1.5e8: return None
        ma50 = c.rolling(50).mean().iloc[-1]; ma200 = c.rolling(200).mean().iloc[-1]
        if price < ma50 or ma50 < ma200: return None

        # --- A. RS 线新高探测 (Relative Strength Blue Dot) ---
        rs_line = c / idx_df
        rs_max_250 = rs_line.rolling(250).max().iloc[-1]
        price_max_250 = h.rolling(250).max().iloc[-1]
        
        # RS 线先于或同步股价创新高 (核心强势指标)
        is_rs_lead = rs_line.iloc[-1] >= rs_max_250 * 0.99
        rs_rank = (rs_line.tail(250) < rs_line.iloc[-1]).mean() * 100

        # --- B. VCP 波动逐级收缩检测 ---
        def get_volatility(days):
            window = df.tail(days)
            return (window['High'].max() - window['Low'].min()) / window['Low'].min() * 100

        v1 = get_volatility(40) # 第一轮收缩
        v2 = get_volatility(20) # 第二轮收缩
        v3 = get_volatility(10) # 第三轮收缩
        # 判断收缩特征：波动率逐渐减小
        is_vcp = v1 > v2 and v2 > v3 and v3 < 6.0

        # --- C. 口袋支点 (Pocket Pivot) 与量能 ---
        avg_v10 = v.rolling(10).mean().iloc[-2]
        is_pivot = v.iloc[-1] > avg_v10 and c.iloc[-1] > c.iloc[-2]
        
        # 吸筹比
        up_vol = v.tail(20)[c.diff().tail(20) > 0].sum()
        dn_vol = v.tail(20)[c.diff().tail(20) < 0].sum()
        ad_ratio = up_vol / (dn_vol + 1)

        # --- D. 盈亏比预估 ---
        atr = (pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)).rolling(14).mean().iloc[-1]
        stop_p = price - (atr * 1.5)
        target_p = price_max_250 * 1.1
        rrr = (target_p - price) / (price - stop_p + 0.01)

        # --- E. 勋章认定逻辑 ---
        tag = "观察"
        if is_vcp and is_rs_lead and is_pivot: tag = "💎 完美起爆点"
        elif is_rs_lead and price > price_max_250 * 0.95: tag = "🥇 相对强度领跑"
        elif mkt_cap > 1000e8 and rs_rank > 80: tag = "🛡️ 蓝筹中流"
        elif is_vcp: tag = "🌪️ 波动收缩中"
        
        if tag == "观察" or rrr < 1.3: return None

        # 最终评分：RS排名(40%) + 吸筹比(30%) + VCP奖励(20%) + RRR(10%)
        vcp_bonus = 20 if is_vcp else 0
        pivot_bonus = 15 if is_pivot else 0
        score = (rs_rank * 0.5) + (min(ad_ratio, 3) * 10) + vcp_bonus + pivot_bonus

        return {
            "tag": tag, "score": float(score), "rs_rank": f"{int(rs_rank)}%",
            "vcp": f"{round(v1,1)}>{round(v2,1)}>{round(v3,1)}", "ad": round(float(ad_ratio), 1),
            "rrr": round(float(rrr), 1), "pivot": "🔥" if is_pivot else "❌",
            "stop": round(float(stop_p), 2), "target": round(float(target_p), 2)
        }
    except: return None

# ==========================================
# 🚀 3. 主流程
# ==========================================
def run_v52_7_super():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ V52.7 Super Performance 启动...")

    # 1. 大盘动态天气 (MA20 + MA200)
    idx_f = yf.download("000300.SS", period="400d", progress=False)['Close']
    idx_s = idx_f.iloc[:, 0] if isinstance(idx_f, pd.DataFrame) else idx_f
    ma20 = idx_s.rolling(20).mean().iloc[-1]; ma200 = idx_s.rolling(200).mean().iloc[-1]; curr_idx = idx_s.iloc[-1]
    
    if curr_idx > ma20 and curr_idx > ma200: mkt_weather = "☀️ 极佳"
    elif curr_idx < ma20 and curr_idx < ma200: mkt_weather = "🌧️ 避险"
    else: mkt_weather = "⛅ 震荡"

    # 2. 获取池
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "close"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 85e8}],
               "range": [0, 1000], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])

    # 3. 扫描
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    chunk_size = 50
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
        for t in chunk:
            try:
                df_h = data[t].dropna() if isinstance(data.columns, pd.MultiIndex) else data.dropna()
                if len(df_h) < 200: continue
                c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                res = calculate_vcp_engine(df_h, idx_s, row_info['mkt'])
                if res:
                    all_hits.append({
                        "代码": c_code, "名称": row_info['name'], "勋章": res['tag'], "评分": res['score'],
                        "RS排名": res['rs_rank'], "VCP序列": res['vcp'], "口袋支点": res['pivot'],
                        "吸筹比": res['ad'], "盈亏比": res['rrr'], "行业": row_info['industry'],
                        "现价": row_info['price'], "止损": res['stop'], "目标": res['target']
                    })
            except: continue

    # 4. 排序与输出
    if not all_hits: return print("无信号")
    final_df = pd.DataFrame(all_hits).sort_values(by="评分", ascending=False)
    final_df = final_df.groupby("行业").head(4) # 行业平衡
    final_df['评分'] = final_df['评分'].rank(pct=True).apply(lambda x: int(x*99))
    final_df = final_df.sort_values(by="评分", ascending=False).head(60)

    sh = init_sheet(); sh.clear()
    cols = ["代码", "名称", "勋章", "评分", "RS排名", "VCP序列", "口袋支点", "吸筹比", "盈亏比", "行业", "现价", "止损", "目标"]
    sh.update(range_name="A1", values=[cols] + final_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("N1", f"V52.7 Super | 天气:{mkt_weather} | {now_str}")
    print(f"🎉 扫描圆满成功！已筛选 {len(final_df)} 只个股。")

if __name__ == "__main__":
    run_v52_7_super()
