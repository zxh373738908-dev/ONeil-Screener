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
# 🧠 2. V52.4 Alpha 核心演算引擎
# ==========================================
def calculate_alpha_engine(df, idx_df, mkt_cap):
    try:
        if len(df) < 200: return None
        
        c = df['Close'].astype(float); h = df['High'].astype(float)
        l = df['Low'].astype(float); v = df['Volume'].astype(float)
        price = float(c.iloc[-1])

        # --- A. 四维加权 RS 系统 (参考 IBD 评级) ---
        def get_rs(days):
            p_now = price; p_prev = c.iloc[-min(days, len(c))]
            i_now = idx_df.iloc[-1]; i_prev = idx_df.iloc[-min(days, len(idx_df))]
            return (p_now / p_prev) / (i_now / i_prev)

        # 权重：最近1个月(40%) + 3个月(20%) + 6个月(20%) + 12个月(20%)
        rs_score = (get_rs(20)*0.4 + get_rs(60)*0.2 + get_rs(120)*0.2 + get_rs(250)*0.2)
        rs_accel = get_rs(20) / (get_rs(120) + 0.001)

        # --- B. 均线斜率探测 (Slope) ---
        ma50 = c.rolling(50).mean()
        ma200 = c.rolling(200).mean()
        # 计算 ma50 过去 5 天的斜率 (确保不是在阴跌)
        slope_ma50 = (ma50.iloc[-1] - ma50.iloc[-5]) / ma50.iloc[-5] * 100
        
        # 趋势硬指标：价格>MA50*0.98 且 MA50>MA200 且 MA50斜率不为负
        is_trend = price > ma50.iloc[-1] * 0.98 and ma50.iloc[-1] > ma200.iloc[-1] * 0.97 and slope_ma50 > -0.1

        if not is_trend: return None

        # --- C. VCP 紧致度与频率 ---
        tightness = (h.tail(8).max() - l.tail(8).min()) / (l.tail(8).min() + 0.001) * 100
        # 探测过去20天内出现过几次极致缩量 (VDU)
        vdu_count = (v.tail(20) < v.rolling(60).mean().tail(20) * 0.6).sum()
        
        # --- D. 勋章逻辑分类 ---
        tag = "关注"
        if rs_accel > 1.3 and tightness < 3.5: tag = "🚀 爆点奇点"
        elif vdu_count >= 3 and tightness < 2.5: tag = "💎 绝对紧致"
        elif mkt_cap > 1000e8 and rs_score > 1.05: tag = "👑 权重大拿" # 601898 属于此类
        elif rs_score > 1.2: tag = "📈 趋势王者"
        
        if tag == "关注": return None

        # --- E. 压力位预测 ---
        # 寻找过去一年内最高价
        annual_high = h.tail(250).max()
        room_to_high = (annual_high / price - 1) * 100

        # 评分：RS得分为主，紧致度奖励为辅
        total_score = (rs_score * 50) + (max(0, (5 - tightness) * 10)) + (vdu_count * 5)

        return {
            "tag": tag, "score": float(total_score), "rs_idx": round(float(rs_score), 2),
            "tight": round(float(tightness), 2), "vdu_freq": f"{vdu_count}次/20d",
            "room": round(float(room_to_high), 1), "target": round(float(annual_high), 2)
        }
    except: return None

# ==========================================
# 🚀 3. 主程序流程
# ==========================================
def run_v52_alpha():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ V52.4 Alpha 启动...")

    # 1. 指数基准
    idx_raw = yf.download("000300.SS", period="400d", progress=False)['Close']
    idx_series = idx_raw.iloc[:, 0] if isinstance(idx_raw, pd.DataFrame) else idx_raw

    # 2. 获取初选池 (TV 市值前1000)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "industry", "close"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}],
        "range": [0, 1000], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])

    # 3. 批量扫描
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    chunk_size = 50
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 分析区块 {i//chunk_size + 1}...")
        data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                # 兼容单股/多股 MultiIndex
                df_h = data[t].dropna() if isinstance(data.columns, pd.MultiIndex) else data.dropna()
                if len(df_h) < 150: continue
                
                c_code = t.split('.')[0]
                row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                
                res = calculate_alpha_engine(df_h, idx_series, row_info['mkt'])
                if res:
                    all_hits.append({
                        "代码": c_code, "名称": row_info['name'], "勋章": res['tag'], 
                        "评分": res['score'], "综合RS": res['rs_idx'], "前高空间%": res['room'],
                        "紧致度": res['tight'], "缩量频率": res['vdu_freq'], 
                        "行业": row_info['industry'], "现价": row_info['price'], "目标(前高)": res['target']
                    })
            except: continue

    # 4. 格式化输出
    sh = init_sheet(); sh.clear()
    if not all_hits: sh.update_acell("A1", "今日无信号"); return

    res_df = pd.DataFrame(all_hits)
    res_df['评分'] = res_df['评分'].rank(pct=True).apply(lambda x: int(x*99))
    final_df = res_df.sort_values(by="评分", ascending=False).head(80)

    cols = ["代码", "名称", "勋章", "评分", "综合RS", "前高空间%", "紧致度", "缩量频率", "行业", "现价", "目标(前高)"]
    sh.update(range_name="A1", values=[cols] + final_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V52.4 Alpha | RS_Weighted | {now_str}")
    print(f"🎉 任务成功！已筛选 {len(final_df)} 只个股。")

if __name__ == "__main__":
    run_v52_alpha()
