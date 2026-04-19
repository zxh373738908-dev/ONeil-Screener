import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests, collections
import yfinance as yf

# ================= 配置区 =================
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))
START_DATE_REF = "2024-12-31" 
# ==========================================

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client.open_by_key(SS_KEY).worksheet(TARGET_SHEET_NAME)

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 启动 V54.0 Multi-Factor Leader Scan...")

    # 1. 扫描池子
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "industry", "market_cap_basic"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 85e8}], 
        "range": [0, 1000]
    }
    raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    
    tickers, meta = [], {}
    for item in raw_data:
        code = item['s'].split(':')[-1]
        yf_code = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        tickers.append(yf_code)
        meta[yf_code] = {
            "name": item['d'][0], 
            "industry": item['d'][1] or "Others", 
            "mktcap": item['d'][2],
            "symbol": code
        }

    # 2. 批量获取数据
    all_data = yf.download(tickers, period="260d", group_by='ticker', progress=True, threads=True)
    
    # 3. 全局相对强度 (REL) 预计算
    stats = []
    for t in tickers:
        try:
            df = all_data[t].dropna()
            if len(df) < 120: continue
            c = df['Close']
            stats.append({
                "code": t,
                "ret_1d": (c.iloc[-1] / c.iloc[-2]) - 1,
                "ret_5d": (c.iloc[-1] / c.iloc[-5]) - 1,
                "ret_20d": (c.iloc[-1] / c.iloc[-20]) - 1,
                "ret_60d": (c.iloc[-1] / c.iloc[-60]) - 1,
                "ret_120d": (c.iloc[-1] / c.iloc[-120]) - 1,
                "ret_ytd": (c.iloc[-1] / c.loc[c.index >= START_DATE_REF].iloc[0]) - 1 if any(c.index >= START_DATE_REF) else 0
            })
        except: continue
    
    full_df = pd.DataFrame(stats)
    for p in [5, 20, 60, 120]:
        full_df[f'REL{p}'] = full_df[f'ret_{p}d'].rank(pct=True) * 99
    full_df['Final_Rank'] = (full_df['REL60'] * 0.4 + full_df['REL20'] * 0.4 + full_df['REL120'] * 0.2)

    # 4. 详细指标扫描
    results = []
    for t in tickers:
        try:
            df = all_data[t].dropna()
            if len(df) < 120 or t not in full_df['code'].values: continue
            
            c, v, h, l = df['Close'], df['Volume'], df['High'], df['Low']
            curr_p = float(c.iloc[-1])
            ma20, ma50, ma120 = c.rolling(20).mean().iloc[-1], c.rolling(50).mean().iloc[-1], c.rolling(120).mean().iloc[-1]
            ma60 = c.rolling(60).mean().iloc[-1]
            
            # --- 新增指标计算 ---
            # ADR (20日平均日行波幅百分比)
            adr = ((h - l) / l).rolling(20).mean().iloc[-1] * 100
            # Vol_Ratio (量比)
            vol_ratio = v.iloc[-1] / v.iloc[-5:-1].mean()
            # Bias (20日乖离率)
            bias = ((curr_p - ma20) / ma20) * 100
            # Resonance (三线共振)
            resonance = "Triple" if curr_p > ma20 > ma50 > ma120 else "None"
            # 60D Trend
            trend_60 = "Up" if curr_p > ma60 and ma60 > c.rolling(60).mean().iloc[-10] else "Consol"
            
            # REL 数据
            row = full_df[full_df['code'] == t].iloc[0]
            
            # Action 逻辑
            volat_5 = (c.iloc[-5:].std() / c.iloc[-5:].mean()) * 100
            if volat_5 < 2.5 and vol_ratio < 1.0: action = "Setup"
            elif curr_p > h.iloc[-2] and vol_ratio > 1.5: action = "Breakout"
            else: action = "Hold"

            # Score 综合评分
            score = row['Final_Rank'] + (15 if action == "Setup" else 0) + (10 if resonance == "Triple" else 0)

            # 过滤：只看高强度或有信号的
            if row['Final_Rank'] > 75 or action != "Hold":
                results.append({
                    "Ticker": meta[t]['symbol'],
                    "Industry": meta[t]['industry'],
                    "Score": int(score),
                    "1D%": f"{round(row['ret_1d']*100, 2)}%",
                    "60D Trend": trend_60,
                    "Action": action,
                    "Resonance": resonance,
                    "ADR": round(adr, 2),
                    "Vol_Ratio": round(vol_ratio, 2),
                    "Bias": f"{round(bias, 1)}%",
                    "MktCap": f"{round(meta[t]['mktcap']/1e8, 1)}亿",
                    "Rank": int(row['Final_Rank']),
                    "REL5": int(row['REL5']),
                    "REL20": int(row['REL20']),
                    "REL60": int(row['REL60']),
                    "REL120": int(row['REL120']),
                    "R20": round(curr_p / c.iloc[-20], 3),
                    "R60": round(curr_p / c.iloc[-60], 3),
                    "R120": round(curr_p / c.iloc[-120], 3),
                    "Price": round(curr_p, 2),
                    "From 2024-12-31": f"{round(row['ret_ytd']*100, 1)}%"
                })
        except: continue

    # 5. 写入 Google Sheets
    sh = init_sheet()
    sh.clear()
    if results:
        df_final = pd.DataFrame(results).sort_values("Score", ascending=False).head(60)
        # 强制列顺序
        cols = ["Ticker", "Industry", "Score", "1D%", "60D Trend", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "Rank", "REL5", "REL20", "REL60", "REL120", "R20", "R60", "R120", "Price", "From 2024-12-31"]
        df_final = df_final[cols]
        sh.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        print(f"✅ 更新完成！已推送 {len(df_final)} 只标的。")

if __name__ == "__main__":
    run_v53_optimizer()
