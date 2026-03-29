import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
import yfinance as yf

# 基础干扰屏蔽
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
        return doc.worksheet("A-Share V39-Titan")
    except:
        return doc.add_worksheet(title="A-Share V39-Titan", rows=1000, cols=20)

# ==========================================
# 🧠 2. 核心算法引擎
# ==========================================
def analyze_v39_logic(df, rs_val, mkt_cap, m_regime):
    try:
        sub_df = df.tail(150).copy()
        c = sub_df['Close'].values; h = sub_df['High'].values; l = sub_df['Low'].values; v = sub_df['Volume'].values; o = sub_df['Open'].values
        price = c[-1]
        
        # VCP 3.0
        v_std_3 = np.std((h[-3:] - l[-3:]) / l[-3:] * 100)
        v_std_10 = np.std((h[-10:] - l[-10:]) / l[-10:] * 100)
        is_coiling = v_std_3 < v_std_10 * 0.75
        
        pivot_p = np.max(h[-10:-1])
        is_break_pivot = price > pivot_p
        
        # 机构吸筹
        entity_ratio = abs(c - o) / (h - l + 0.001)
        niv_short = (np.sign(c[-10:] - o[-10:]) * v[-10:] * entity_ratio[-10:]).sum()
        
        ma50 = sub_df['Close'].rolling(50).mean().iloc[-1]
        
        # 战法判定
        tag = "🛡️观察"
        if is_coiling and is_break_pivot and niv_short > 0: tag = "🌅黎明起爆"
        elif mkt_cap > 800e8 and niv_short > 0 and price > ma50: tag = "🛡️基石反弹"
        elif rs_val > 1.2: tag = "🌪️主升加速"

        score = (rs_val * 35) + (20 if niv_short > 0 else 0) + (20 if is_coiling else 0)
        if m_regime == "DOWN": score *= 0.8

        return tag, round(score, 1), round(pivot_p, 2)
    except:
        return "ERR", 0, 0

# ==========================================
# 🚀 3. 主扫描流程 (含降级保护逻辑)
# ==========================================
def run_v39_resilient():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V39.5 坚守版启动 (带 API 降级保护)...")

    cols = ["Ticker", "Name", "综合评分", "战术勋章", "黎明枢轴", "行业", "RS强度/涨幅", "Price"]

    # 1. 大盘探测 (带容错)
    m_regime = "UP"
    try:
        idx = yf.download("000300.SS", period="50d", progress=False)
        if not idx.empty:
            close = idx['Close'].iloc[-1].values[0] if isinstance(idx['Close'].iloc[-1], pd.Series) else idx['Close'].iloc[-1]
            ma50 = idx['Close'].rolling(50).mean().iloc[-1]
            m_regime = "UP" if close > ma50 else "DOWN"
            print(f" -> 🚦 大盘状态: {m_regime}")
    except:
        print(" -> ⚠️ 无法获取大盘数据，默认 UP 模式运行")

    # 2. 从 TradingView 获取全市场活跃名单 (此接口 GitHub 访问极稳)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "close", "change"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}],
               "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    
    try:
        raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([
            {"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4], "chg": d['d'][5]} 
            for d in raw_data
        ])
        print(f" -> ✅ 获取 TV 标的: {len(df_pool)} 只")
    except:
        return print("❌ 无法访问 TV 接口，网络彻底断开")

    # 3. Yahoo 数据拉取与演算
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    all_hits = []
    chunk_size = 30 # 缩小分块以规避拦截
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        try:
            # 伪装 Header 请求
            data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True, timeout=10)
            
            for t in chunk:
                try:
                    if t not in data.columns.get_level_values(0): continue
                    df_h = data[t].dropna()
                    if len(df_h) < 100: continue
                    
                    p = df_h['Close'].iloc[-1]
                    # 计算 RS (120日相对)
                    rs_raw = (p / df_h['Close'].iloc[-120]) if len(df_h)>120 else 1.0
                    
                    c_code = t.split('.')[0]
                    row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                    tag, score, pivot = analyze_v39_logic(df_h, rs_raw, row_info['mkt'], m_regime)
                    
                    all_hits.append({
                        "Ticker": c_code, "Name": row_info['name'], "综合评分": score, "战术勋章": tag, 
                        "黎明枢轴": pivot, "行业": row_info['industry'], "RS强度/涨幅": round(rs_raw, 2), "Price": round(float(p), 2)
                    })
                except: continue
        except:
            print(f" -> ⚠️ 块 {i//chunk_size + 1} Yahoo 连接受阻，跳过...")
            continue

    # 4. 写入与降级保护核心
    sh = init_sheet(); sh.clear()
    
    if not all_hits:
        print("🚨 致命拦截：Yahoo 接口全线崩溃！启动降级通道 (TV-Direct)...")
        # 降级模式：直接使用 TradingView 传回的当日涨幅和价格
        fallback_list = []
        for _, row in df_pool.head(50).iterrows():
            fallback_list.append({
                "Ticker": row['code'], "Name": row['name'], "综合评分": "紧急降级", "战术勋章": "📻TV直连模式",
                "黎明枢轴": "无数据", "行业": row['industry'], "RS强度/涨幅": f"{round(row['chg'], 2)}%", "Price": row['price']
            })
        final_df = pd.DataFrame(fallback_list)
        diag_msg = "⚠️ Yahoo 接口被拦截，当前显示 TradingView 实时涨幅榜 (前50)。"
    else:
        # 标准模式：按照 RS 强度和评分排序
        final_df = pd.DataFrame(all_hits).sort_values(by="综合评分", ascending=False).head(50)
        diag_msg = f"✅ 数据链路正常。获取到 {len(all_hits)} 个泰坦深度分析目标。"

    # 5. 安全写入
    for col in cols:
        if col not in final_df.columns: final_df[col] = ""
    
    sh.update(range_name="A1", values=[cols] + final_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"状态: {diag_msg}")
    sh.update_acell("L2", f"Last Update (BJ): {now_str}")
    
    print(f"🎉 V39.5 任务完成！")

if __name__ == "__main__":
    run_v39_resilient()
