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
# 🧠 2. V52.8 Alpha King 核心引擎
# ==========================================
def calculate_alpha_king_engine(df, idx_df, mkt_cap):
    try:
        if len(df) < 200: return None
        
        c = df['Close'].astype(float); h = df['High'].astype(float)
        l = df['Low'].astype(float); v = df['Volume'].astype(float)
        price = float(c.iloc[-1])
        
        # 0. 基础过滤：流动性与 Stage 2 趋势
        if (c * v).tail(20).mean() < 1.5e8: return None
        ma10 = c.rolling(10).mean().iloc[-1]
        ma20 = c.rolling(20).mean().iloc[-1]
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        
        # 核心趋势要求：价格必须在MA50上方，且MA50 > MA200
        if price < ma50 or ma50 < ma200: return None

        # --- A. RS 核心 (IBD 风格横向分值) ---
        rs_val = ( (price/c.iloc[-21])*0.4 + (price/c.iloc[-63])*0.2 + (price/c.iloc[-126])*0.2 + (price/c.iloc[-252])*0.2 )
        # RS 线是否创新高 (Blue Dot)
        rs_line = c / idx_df
        is_rs_blue_dot = rs_line.iloc[-1] >= rs_line.tail(250).max() * 0.99

        # --- B. 模态识别：VCP vs Power Trend ---
        def get_v(days): return (h.tail(days).max() - l.tail(days).min()) / l.tail(days).min() * 100
        v1, v2, v3 = get_v(40), get_v(20), get_v(10)
        
        is_vcp = v1 > v2 and v2 > v3 and v3 < 6.0 # 经典收缩
        is_power_trend = price > ma10 > ma20 > ma50 # 强力多头
        
        # --- C. 量能质量 ---
        # 过去20天收盘价上涨日的成交量 / 下跌日成交量
        ad_ratio = v.tail(20)[c.diff().tail(20) > 0].sum() / (v.tail(20)[c.diff().tail(20) < 0].sum() + 1)
        is_pocket_pivot = v.iloc[-1] > v.tail(10).mean().iloc[-2] and c.iloc[-1] > c.iloc[-2]

        # --- D. 动态风险收益比 ---
        atr = (pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)).rolling(14).mean().iloc[-1]
        stop_p = price - max(atr * 1.5, price * 0.04) # 止损不小于4%
        target_p = h.tail(250).max() * (1.1 if price > h.tail(250).max() else 1.0)
        rrr = (target_p - price) / (price - stop_p + 0.01)

        # --- E. 勋章认定 ---
        tag = "关注"
        if is_vcp and is_rs_blue_dot: tag = "🥇 完美VCP"
        elif is_power_trend and is_rs_blue_dot: tag = "🚀 电力爆发(强趋势)"
        elif is_rs_blue_dot: tag = "🔹 RS先行点"
        elif is_vcp: tag = "🌪️ 波动收缩中"
        
        if tag == "关注" or rrr < 1.0: return None

        return {
            "tag": tag, "score": float(rs_val), "rs_val": rs_val, "blue": "✅" if is_rs_blue_dot else "❌",
            "vcp_v": f"{round(v3,1)}%", "ad": round(float(ad_ratio), 1), "rrr": round(float(rrr), 1),
            "pivot": "🔥" if is_pocket_pivot else "❌", "stop": round(float(stop_p), 2), "target": round(float(target_p), 2)
        }
    except: return None

# ==========================================
# 🚀 3. 主程序流程
# ==========================================
def run_v52_8_alpha():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ V52.8 Alpha King 启动...")

    # 1. 大盘基准与宽度计算
    idx_f = yf.download("000300.SS", period="400d", progress=False)['Close']
    idx_s = idx_f.iloc[:, 0] if isinstance(idx_f, pd.DataFrame) else idx_f
    
    # 2. 获取池并下载数据 (用于宽度计算和横向RS排名)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "close"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 85e8}],
               "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])

    # 3. 执行全池扫描
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    # 计算市场宽度 (有多少个股在MA50之上)
    above_ma50_count = 0
    valid_stocks_count = 0

    temp_results = []
    for t in tickers:
        try:
            df_h = data[t].dropna()
            if len(df_h) < 150: continue
            valid_stocks_count += 1
            if df_h['Close'].iloc[-1] > df_h['Close'].rolling(50).mean().iloc[-1]: above_ma50_count += 1
            
            c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
            res = calculate_alpha_king_engine(df_h, idx_s, row_info['mkt'])
            if res:
                res.update({"code": c_code, "name": row_info['name'], "industry": row_info['industry'], "price": row_info['price']})
                temp_results.append(res)
        except: continue

    # 4. 计算横向 RS 评级 (0-99)
    if not temp_results: return print("无信号")
    final_raw_df = pd.DataFrame(temp_results)
    final_raw_df['RS评级'] = final_raw_df['score'].rank(pct=True).apply(lambda x: int(x*99))
    
    # 市场宽度天气
    breadth = above_ma50_count / valid_stocks_count * 100
    weather = "☀️ 极佳" if breadth > 60 else "⛅ 震荡" if breadth > 40 else "🌧️ 避险"

    # 5. 行业平衡与最终输出
    final_df = final_raw_df[final_raw_df['RS评级'] > 70].sort_values(by="RS评级", ascending=False)
    final_df = final_df.groupby("industry").head(4) # 限制每个行业4只
    final_df = final_df.head(60)

    sh = init_sheet(); sh.clear()
    cols_map = {"code": "代码", "name": "名称", "tag": "勋章", "RS评级": "RS评级", "blue": "RS新高", 
                "vcp_v": "收缩压", "pivot": "口袋点", "ad": "吸筹比", "rrr": "盈亏比", 
                "industry": "行业", "price": "现价", "stop": "止损", "target": "目标"}
    final_df = final_df.rename(columns=cols_map)
    sh.update(range_name="A1", values=[list(cols_map.values())] + final_df[list(cols_map.values())].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("N1", f"V52.8 Alpha King | 宽度:{int(breadth)}% {weather} | {now_str}")
    print(f"🎉 任务成功！已筛选 {len(final_df)} 只高评级个股。")

if __name__ == "__main__":
    run_v52_8_alpha()
