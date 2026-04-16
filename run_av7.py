import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests
import yfinance as yf

# ================= 配置区 =================
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

# 调整后的早鸟参数 (更符合 A 股弹性)
RS_MIN = 70           # RS 评级下调至 70，扩大扫描范围
TIGHTNESS_MAX = 5.0   # 紧致度放宽到 5% (A股波动大，太小了选不到)
DIST_MA20_LIMIT = 7.0 # 距离20日线 7% 以内
# ==========================================

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client.open_by_key(SS_KEY).worksheet(TARGET_SHEET_NAME)

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 开始高胜率早鸟扫描 (自适应版)...")

    # 1. 抓取数据
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "industry", "close", "market_cap_basic"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}], 
        "range": [0, 800]
    }
    raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    
    tickers, meta = [], {}
    for item in raw_data:
        code = item['s'].split(':')[-1]
        yf_code = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        tickers.append(yf_code)
        meta[yf_code] = {"name": item['d'][1], "industry": item['d'][2], "symbol": code}

    # 2. 下载数据
    print(f"📥 正在分析 {len(tickers)} 只核心资产...")
    all_data = yf.download(tickers, period="260d", group_by='ticker', progress=True, threads=True)
    
    # 3. 计算 RS 评级
    rs_list = []
    for t in tickers:
        try:
            c = all_data[t]['Close'].dropna()
            if len(c) < 120: continue
            score = (c.iloc[-1]/c.iloc[-22]*0.4) + (c.iloc[-1]/c.iloc[-66]*0.2) + (c.iloc[-1]/c.iloc[-132]*0.2) + (c.iloc[-1]/c.iloc[-250]*0.2)
            rs_list.append({"code": t, "score": score})
        except: continue
    rs_df = pd.DataFrame(rs_list)
    rs_df['rank'] = rs_df['score'].rank(pct=True) * 99

    # 4. 核心扫描逻辑
    results = []
    candidates = [] # 存储 RS 高但紧致度略差的“强力股”

    for t in tickers:
        try:
            df = all_data[t].dropna()
            if len(df) < 50: continue
            
            close = df['Close']
            curr_p = float(close.iloc[-1])
            ma20 = close.rolling(20).mean().iloc[-1]
            ma50 = close.rolling(50).mean().iloc[-1]
            
            # 计算指标
            this_rs = rs_df[rs_df['code'] == t]['rank'].values[0]
            tightness = (close.iloc[-5:].std() / close.iloc[-5:].mean()) * 100
            bias_20 = ((curr_p - ma20) / ma20) * 100
            vol_ratio = df['Volume'].iloc[-1] / df['Volume'].iloc[-5:-1].mean()
            
            # 数据封装
            item = {
                "RS评级": int(this_rs),
                "代码": meta[t]['symbol'],
                "名称": meta[t]['name'],
                "现价": round(curr_p, 2),
                "紧致度": f"{round(tightness, 2)}%",
                "量比": round(vol_ratio, 2),
                "MA20乖离": f"{round(bias_20, 1)}%",
                "行业": meta[t]['industry'],
                "更新": now.strftime('%H:%M')
            }

            # 严格筛选 (早鸟起飞点)
            if this_rs > RS_MIN and -2 < bias_20 < DIST_MA20_LIMIT and tightness < TIGHTNESS_MAX:
                item["状态"] = "🔥 极度蓄势"
                results.append(item)
            # 次优筛选 (RS极高但已经在涨)
            elif this_rs > 90 and bias_20 < 15:
                item["状态"] = "⭐ 领涨强势"
                candidates.append(item)

        except: continue

    # 5. 写入逻辑
    sh = init_sheet()
    sh.clear()
    
    # 优先展示“蓄势”标的，如果没有，展示“RS前20名”的强势股
    final_list = results if results else sorted(candidates, key=lambda x: x['RS评级'], reverse=True)[:20]

    if final_list:
        df_res = pd.DataFrame(final_list)
        sh.update([df_res.columns.values.tolist()] + df_res.values.tolist())
        print(f"✅ 成功! 发现 {len(results)} 只完美蓄势股，以及 {len(final_list)-len(results)} 只高RS领涨股。")
    else:
        print("⚠️ 依然没有发现任何高价值标的，请检查数据源或市场是否处于普跌。")

if __name__ == "__main__":
    run_v53_optimizer()
