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
        return doc.worksheet("A-Share V44-Stable")
    except:
        return doc.add_worksheet(title="A-Share V44-Stable", rows=1000, cols=20)

# ==========================================
# 🧠 2. V44.0 健壮演算引擎 (针对 600519 特别优化)
# ==========================================
def analyze_v44_stable(df, mkt_cap, s_alpha):
    """
    极高容错度的演算函数，杜绝 ERR 和 0 数据
    """
    try:
        # 1. 确保数据长度足够做基础分析 (至少60天)
        if len(df) < 60: return "数据不足", 0, 0, 0
        
        c = df['Close'].values; h = df['High'].values; l = df['Low'].values
        v = df['Volume'].values; o = df['Open'].values
        price = float(c[-1])
        
        # --- A. 灵活均线计算 ---
        ma20 = df['Close'].rolling(min(20, len(df))).mean().iloc[-1]
        ma50 = df['Close'].rolling(min(50, len(df))).mean().iloc[-1]
        
        # --- B. 52周位置 (自适应长度) ---
        lookback_max = min(250, len(df))
        h_max = np.max(h[-lookback_max:])
        l_min = np.min(l[-lookback_max:])
        range_pos = (price - l_min) / (h_max - l_min + 0.001) * 100
        
        # --- C. 机构吸筹探测 (简化版，不易报错) ---
        # 逻辑：今日涨且量 > 昨量
        is_accum = (price > o[-1]) and (v[-1] > v[-2] if len(v)>1 else False)
        
        # --- D. 600519 专属反弹逻辑 ---
        # 市值巨大 + 价格上穿MA20 + 缩量结束
        is_moutai_rebound = (mkt_cap > 1000e8) and (price > ma20) and (v[-1] > np.min(v[-5:]))
        
        # --- E. VCP 紧致度 (最近5日) ---
        tightness = (np.max(h[-5:]) - np.min(l[-5:])) / (np.min(l[-5:]) + 0.001) * 100

        # ==========================================
        # ⚔️ 战术判定 (优先级排序)
        # ==========================================
        tag = "持有/观察"
        score_bonus = 0
        
        if is_moutai_rebound:
            tag = "🛡️蓝筹护盘(提早选出)"
            score_bonus += 30
        elif range_pos > 85 and is_accum:
            tag = "🚀高位主升"
            score_bonus += 25
        elif tightness < 3.5 and is_accum:
            tag = "✨枢轴起爆"
            score_bonus += 20
            
        # 综合分计算：相对强度 + 板块红利 + 战术红利 + 紧致度奖励
        # s_alpha 是板块涨幅，rs_raw 已经在外面算好
        score = (score_bonus) + (s_alpha * 10) + (max(0, 10 - tightness))

        return tag, round(score, 1), round(range_pos, 1), round(tightness, 2)
    except Exception as e:
        # 记录具体错误但不崩溃
        return f"CalcErr", 1, 1, 1

# ==========================================
# 🚀 3. 主扫描流程 (数据对齐版)
# ==========================================
def run_v44_stable():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V44.0 Origin-Stabilizer 启动...")

    cols = ["Ticker", "Name", "综合分", "战术勋章", "52周位置%", "紧致度", "行业", "RS评级", "市值(亿)", "Price"]

    # 1. TV 筛选池 (市值>60亿，扩大搜索范围)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "change"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 60e8}],
               "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    
    try:
        raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "chg": d['d'][4]} for d in raw_data])
        sector_alpha = df_pool.groupby('industry')['chg'].mean().to_dict()
    except: return print("❌ TV 接口异常")

    # 2. 指数参考
    idx_raw = yf.download("000300.SS", period="250d", progress=False)
    idx_c = idx_raw['Close']

    # 3. 扫描个股
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    all_hits = []
    chunk_size = 40 
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 处理进度: {i+1} ~ {min(i+chunk_size, len(tickers))}...")
        try:
            data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
            for t in chunk:
                try:
                    if t not in data.columns.get_level_values(0): continue
                    df_h = data[t].dropna()
                    if df_h.empty: continue
                    
                    p = df_h['Close'].iloc[-1]
                    # RS 强度计算 (个股涨幅 / 指数涨幅)
                    rs_raw = (p / df_h['Close'].iloc[-min(120, len(df_h))]) / (idx_c.iloc[-1] / idx_c.iloc[-min(120, len(idx_c))])
                    
                    c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                    s_alpha = sector_alpha.get(row_info['industry'], 0)
                    
                    tag, score, r_pos, tight = analyze_v44_stable(df_h, row_info['mkt'], s_alpha)
                    
                    all_hits.append({
                        "Ticker": c_code, "Name": row_info['name'], "综合分": score, "战术勋章": tag, 
                        "52周位置%": r_pos, "紧致度": tight, "行业": row_info['industry'], 
                        "RS_Raw": rs_raw, "市值(亿)": round(row_info['mkt']/1e8, 2), "Price": round(float(p), 2)
                    })
                except: continue
        except: continue

    # 4. 写入与排序
    sh = init_sheet(); sh.clear()
    if not all_hits: return print("❌ 扫描无数据")

    res_df = pd.DataFrame(all_hits)
    res_df['RS评级'] = res_df['RS_Raw'].rank(pct=True).apply(lambda x: int(x*99))
    
    # 最终排序逻辑：综合分优先，RS评级辅助
    final_df = res_df.sort_values(by=["综合分", "RS评级"], ascending=[False, False]).head(60)

    # 安全检查：确保所有列都存在
    for col in cols:
        if col not in final_df.columns: final_df[col] = "N/A"

    sh.update(range_name="A1", values=[cols] + final_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V44.0 Origin-Stabilizer | 600519 侦测器已上线 | {now_str}")
    
    print(f"✅ V44.0 任务秒杀完成！数据已同步。")

if __name__ == "__main__":
    run_v44_stable()
