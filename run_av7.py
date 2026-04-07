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
    if not os.path.exists(CREDS_FILE): print(f"❌ 错误: 找不到 {CREDS_FILE}"); exit(1)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_key(SS_KEY)
        return doc.worksheet(TARGET_SHEET_NAME) if TARGET_SHEET_NAME in [w.title for w in doc.worksheets()] else doc.add_worksheet(TARGET_SHEET_NAME, 1000, 20)
    except Exception as e: print(f"❌ 授权失败: {e}"); exit(1)

# ==========================================
# 🧠 2. V52.9 Alpha Apex 巅峰引擎
# ==========================================
def calculate_alpha_apex_engine(df, idx_df, mkt_cap):
    try:
        if len(df) < 200: return None
        c = df['Close'].astype(float); h = df['High'].astype(float)
        l = df['Low'].astype(float); v = df['Volume'].astype(float)
        price = float(c.iloc[-1])
        
        # 0. 流动性过滤
        if (c * v).tail(20).mean() < 1.5e8: return None
        ma50 = c.rolling(50).mean().iloc[-1]; ma200 = c.rolling(200).mean().iloc[-1]
        if price < ma50 or ma50 < ma200: return None

        # --- A. RS 强化系统 (IBD横向+RS线斜率) ---
        rs_val = ( (price/c.iloc[-21])*0.45 + (price/c.iloc[-63])*0.2 + (price/c.iloc[-126])*0.2 + (price/c.iloc[-252])*0.15 )
        rs_line = c / idx_df
        # RS 线 5 日斜率
        rs_slope = (rs_line.iloc[-1] - rs_line.iloc[-6]) / rs_line.iloc[-6] * 100
        is_blue_dot = rs_line.iloc[-1] >= rs_line.tail(250).max() * 0.99

        # --- B. 紧致度与 VDU (缩量寂静) ---
        tightness_8 = (h.tail(8).max() - l.tail(8).min()) / l.tail(8).min() * 100
        # 探测过去 5 天是否有至少 1 天成交量 < 60日均量的 55%
        vdu_signal = (v.tail(5) < v.rolling(60).mean().tail(5) * 0.55).any()
        
        # --- C. 乖离率检查 (防追高) ---
        bias_50 = (price / ma50 - 1) * 100
        # 如果 bias_50 > 25%, 标记为极度过热
        is_extended = bias_50 > 22

        # --- D. 模态判定 ---
        tag = "关注"
        if tightness_8 < 4 and vdu_signal and is_blue_dot: tag = "💎 巅峰奇点(VDU)"
        elif price > c.rolling(10).mean().iloc[-1] > c.rolling(20).mean().iloc[-1] and is_blue_dot: tag = "🚀 强力主升"
        elif is_blue_dot: tag = "🔹 RS领跑"
        elif tightness_8 < 2.5: tag = "🌪️ 极致紧致"
        
        if tag == "关注" or price < c.iloc[-2]: return None # 过滤掉今日收跌的标的

        # --- E. 止损与空间 ---
        atr = (pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)).rolling(14).mean().iloc[-1]
        stop_p = price - (atr * 1.6)
        target_p = h.tail(250).max() * 1.12
        rrr = (target_p - price) / (price - stop_p + 0.01)

        return {
            "tag": tag, "score": float(rs_val), "rs_val": rs_val, "rs_slope": round(rs_slope, 2),
            "bias": round(bias_50, 1), "vdu": "✅" if vdu_signal else "❌", "rrr": round(rrr, 1),
            "stop": round(stop_p, 2), "target": round(target_p, 2), "is_ext": is_extended
        }
    except: return None

# ==========================================
# 🚀 3. 主流程流程
# ==========================================
def run_v52_9_apex():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ V52.9 Alpha Apex 启动...")

    # 1. 基准
    idx_f = yf.download("000300.SS", period="400d", progress=False)['Close']
    idx_s = idx_f.iloc[:, 0] if isinstance(idx_f, pd.DataFrame) else idx_f
    
    # 2. 池获取
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "close"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 85e8}],
               "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])

    # 3. 批量扫描
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    for t in tickers:
        try:
            df_h = data[t].dropna()
            if len(df_h) < 150: continue
            c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
            res = calculate_alpha_apex_engine(df_h, idx_s, row_info['mkt'])
            if res:
                res.update({"code": c_code, "name": row_info['name'], "industry": row_info['industry'], "price": row_info['price']})
                all_hits.append(res)
        except: continue

    if not all_hits: return print("无信号")
    
    # 4. 行业 RS 分析 & RS 横向评级
    final_raw_df = pd.DataFrame(all_hits)
    final_raw_df['RS评级'] = final_raw_df['score'].rank(pct=True).apply(lambda x: int(x*99))
    
    # 计算行业平均 RS，识别领涨板块
    industry_rank = final_raw_df.groupby("industry")['RS评级'].mean().sort_values(ascending=False)
    def get_leader_tag(ind):
        return "🔥 领涨主线" if industry_rank[ind] > 85 else "普通"
    final_raw_df['板块地位'] = final_raw_df['industry'].apply(get_leader_tag)

    # 5. 排序与输出 (过滤 RS < 75)
    final_df = final_raw_df[final_raw_df['RS评级'] > 75].sort_values(by="RS评级", ascending=False)
    final_df = final_df.groupby("industry").head(5).head(60)

    # 处理乖离率警告
    final_df['勋章'] = final_df.apply(lambda r: "⚠️ 极致乖离" if r['is_ext'] else r['tag'], axis=1)

    sh = init_sheet(); sh.clear()
    cols_map = {"code": "代码", "name": "名称", "勋章": "勋章", "RS评级": "RS评级", "rs_slope": "RS斜率",
                "bias": "50日乖离", "vdu": "缩量", "rrr": "盈亏比", "板块地位": "板块地位", 
                "industry": "行业", "price": "现价", "stop": "止损", "target": "目标"}
    sh.update(range_name="A1", values=[list(cols_map.values())] + final_df[list(cols_map.values())].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("N1", f"V52.9 Alpha Apex | 横向RS+板块扫描 | {now_str}")
    print(f"🎉 扫描圆满成功！已更新 {len(final_df)} 只个股。")

if __name__ == "__main__":
    run_v52_9_apex()
