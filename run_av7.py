import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
import yfinance as yf

# 环境屏蔽
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# 配置
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-Wednesday-Sniper"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try:
        return doc.worksheet(TARGET_SHEET_NAME)
    except:
        return doc.add_worksheet(TARGET_SHEET_NAME, 1000, 20)

def run_wednesday_sniper():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ 正在扫描潜伏标的...")
    
    # 1. 抓取指数基准
    idx_raw = yf.download("000300.SS", period="400d", progress=False)['Close']
    idx_s = idx_raw.iloc[:, 0] if isinstance(idx_raw, pd.DataFrame) else idx_raw
    
    # 2. 获取股票池
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "close"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}],
               "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "industry": d['d'][3]} for d in resp])
        tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
        print(f"✅ 成功获取 {len(tickers)} 只标的，正在下载历史行情...")
    except Exception as e:
        print(f"❌ 获取股票池失败: {e}"); return

    # 3. 批量下载行情
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    # 4. 循环扫描 (核心逻辑)
    all_hits = []
    for t in tickers:
        try:
            df = data[t].dropna()
            if len(df) < 250: continue
            
            c = df['Close'].astype(float)
            v = df['Volume'].astype(float)
            price = c.iloc[-1]
            
            # 逻辑：RS新高 + 缩量 + 均线之上
            rs_line = c / idx_s
            is_rs_lead = rs_line.iloc[-1] >= rs_line.tail(250).max() * 0.95
            is_vol_shrink = v.iloc[-1] < v.rolling(20).mean().iloc[-1] * 0.9
            
            if is_rs_lead and is_vol_shrink:
                code = t.split('.')[0]
                name = df_pool[df_pool['code'] == code].iloc[0]['name']
                all_hits.append({
                    "代码": code, "名称": name, "现价": round(price, 2), 
                    "RS强度": "✅新高", "缩量": "✅"
                })
        except: continue

    # 5. 更新表格
    if all_hits:
        sh = init_sheet()
        sh.clear()
        df_final = pd.DataFrame(all_hits)
        sh.update(range_name="A1", values=[df_final.columns.tolist()] + df_final.values.tolist(), value_input_option="USER_ENTERED")
        print(f"🎉 成功！已更新 {len(all_hits)} 只潜伏标的到表格。")
    else:
        print("⚠️ 未发现符合条件的潜伏标的。")

if __name__ == "__main__":
    run_wednesday_sniper()
