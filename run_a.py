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
        return doc.worksheet("A-Share ONeil")
    except:
        return doc.add_worksheet(title="A-Share ONeil", rows=1000, cols=20)

# ==========================================
# 🧠 2. V45.0 战法引擎 (ONeil + 龙回头)
# ==========================================
def analyze_v45_oneil(df, mkt_cap, s_alpha):
    try:
        if len(df) < 60: return "数据不足", 0, 0, 0
        
        c = df['Close'].values; h = df['High'].values; l = df['Low'].values
        v = df['Volume'].values; o = df['Open'].values
        price = float(c[-1])
        
        # --- A. 均线系统 ---
        ma20 = df['Close'].rolling(20).mean().iloc[-1]
        ma50 = df['Close'].rolling(50).mean().iloc[-1]
        ma150 = df['Close'].rolling(150).mean().iloc[-1]
        ma200 = df['Close'].rolling(200).mean().iloc[-1]
        
        # --- B. 欧奈尔趋势模板 (Trend Template) ---
        is_oneil_trend = price > ma50 > ma150 > ma200
        
        # --- C. 52周位置 ---
        lookback_52w = min(250, len(df))
        h52 = np.max(h[-lookback_52w:])
        l52 = np.min(l[-lookback_52w:])
        range_pos = (price - l52) / (h52 - l52 + 0.001) * 100
        
        # --- D. 龙回头策略探测 ---
        # 20日内最高涨幅 > 25%
        past_20d_max = np.max(c[-20:])
        past_20d_start = c[-25] if len(c) > 25 else c[0]
        was_dragon = (past_20d_max / past_20d_start - 1) > 0.25
        # 当前缩量回踩 MA20
        is_dragon_pullback = was_dragon and (price >= ma20 * 0.98) and (price <= ma20 * 1.05) and (v[-1] < np.mean(v[-10:]))
        
        # --- E. 蓝筹支点 (600519) ---
        # 权重大票 + 站稳MA20 + 量能平稳
        is_moutai_pivot = (mkt_cap > 1000e8) and (price > ma20) and (v[-1] > np.min(v[-5:]))
        
        # --- F. VCP 紧致度 ---
        tightness = (np.max(h[-5:]) - np.min(l[-5:])) / (np.min(l[-5:]) + 0.001) * 100

        # --- 战术判定 ---
        tag = "趋势观察"
        bonus = 0
        if is_dragon_pullback:
            tag = "🐲龙回头(缩量止跌)"
            bonus = 40
        elif is_moutai_pivot:
            tag = "🛡️蓝筹复兴(提前选出)"
            bonus = 35
        elif is_oneil_trend and range_pos > 85:
            tag = "🚀欧奈尔主升"
            bonus = 30
        elif tightness < 3.0 and price > ma50:
            tag = "✨枢轴紧缩"
            bonus = 20

        # 评分：战术加成 + 相对强度 + 板块效应
        score = bonus + (s_alpha * 5) + (max(0, 10 - tightness))
        if not is_oneil_trend: score -= 15 # 趋势不佳扣分

        return tag, round(score, 1), round(range_pos, 1), round(tightness, 2)
    except:
        return "计算错误", 0, 0, 0

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v44_stable():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V45.0 ONeil & Dragon 启动...")

    cols = ["Ticker", "Name", "综合分", "RS评级", "战术勋章", "52周位置%", "紧致度", "行业", "市值(亿)", "Price"]

    # 1. 获取 TV 池子
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "change"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 65e8}],
               "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    
    try:
        raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "chg": d['d'][4]} for d in raw_data])
        sector_alpha = df_pool.groupby('industry')['chg'].mean().to_dict()
    except: return print("❌ TV 接口异常")

    # 2. 基准指数
    idx_raw = yf.download("000300.SS", period="250d", progress=False)
    idx_c = idx_raw['Close'].values.flatten()

    # 3. 扫描个股
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    all_hits = []
    chunk_size = 40
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 探测进度: {i+1} ~ {min(i+chunk_size, len(tickers))}...")
        try:
            data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
            for t in chunk:
                try:
                    if t not in data.columns.get_level_values(0): continue
                    df_h = data[t].dropna()
                    if len(df_h) < 60: continue
                    
                    p = float(df_h['Close'].iloc[-1])
                    
                    # --- 核心修复：确保 rs_raw 是纯数字 (Scalar) ---
                    stock_past = float(df_h['Close'].iloc[-min(120, len(df_h))])
                    idx_now = float(idx_c[-1])
                    idx_past = float(idx_c[-min(120, len(idx_c))])
                    rs_raw = (p / stock_past) / (idx_now / idx_past)
                    
                    c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                    s_alpha = sector_alpha.get(row_info['industry'], 0)
                    
                    tag, score, r_pos, tight = analyze_v45_oneil(df_h, row_info['mkt'], s_alpha)
                    
                    all_hits.append({
                        "Ticker": c_code, "Name": row_info['name'], "综合分": score, "战术勋章": tag, 
                        "52周位置%": r_pos, "紧致度": tight, "行业": row_info['industry'], 
                        "RS_Raw": rs_raw, "市值(亿)": round(row_info['mkt']/1e8, 2), "Price": round(p, 2)
                    })
                except Exception as e: continue
        except: continue

    # 4. 排序与写入
    sh = init_sheet(); sh.clear()
    if not all_hits: return print("❌ 扫描结果为空")

    res_df = pd.DataFrame(all_hits)
    
    # 彻底解决 rank 报错：确保 RS_Raw 列全是数字类型且无空值
    res_df['RS_Raw'] = pd.to_numeric(res_df['RS_Raw'], errors='coerce').fillna(0)
    res_df['RS评级'] = res_df['RS_Raw'].rank(pct=True).apply(lambda x: int(x*99))
    
    final_df = res_df.sort_values(by=["综合分", "RS评级"], ascending=[False, False]).head(60)

    sh.update(range_name="A1", values=[cols] + final_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V45.0 ONeil | 🐉龙回头+蓝筹复兴侦测 | {now_str}")
    
    print(f"🎉 V45.0 扫描大功告成！")

if __name__ == "__main__":
    run_v44_stable()
