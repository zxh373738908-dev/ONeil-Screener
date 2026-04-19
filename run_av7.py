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
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 启动 V54.1 Pro-Layout 优化版...")

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
            "ind": (item['d'][1] or "Misc")[:6], # 截断行业名节省空间
            "mktcap": item['d'][2],
            "symbol": code
        }

    # 2. 批量获取数据
    all_data = yf.download(tickers, period="260d", group_by='ticker', progress=True, threads=True)
    
    # 3. REL 预计算
    stats = []
    for t in tickers:
        try:
            df = all_data[t].dropna()
            if len(df) < 130: continue
            c = df['Close']
            stats.append({
                "code": t,
                "r1": (c.iloc[-1] / c.iloc[-2]) - 1,
                "r5": (c.iloc[-1] / c.iloc[-5]) - 1,
                "r20": (c.iloc[-1] / c.iloc[-20]) - 1,
                "r60": (c.iloc[-1] / c.iloc[-60]) - 1,
                "r120": (c.iloc[-1] / c.iloc[-120]) - 1,
                "rytd": (c.iloc[-1] / c.loc[c.index >= START_DATE_REF].iloc[0]) - 1 if any(c.index >= START_DATE_REF) else 0
            })
        except: continue
    
    full_df = pd.DataFrame(stats)
    for p in [5, 20, 60, 120]:
        full_df[f'REL{p}'] = full_df[f'r{p}'].rank(pct=True) * 99
    full_df['Final_Rank'] = (full_df['REL60'] * 0.4 + full_df['REL20'] * 0.4 + full_df['REL120'] * 0.2)

    # 4. 指标扫描与格式化
    results = []
    for t in tickers:
        try:
            df = all_data[t].dropna()
            if len(df) < 120 or t not in full_df['code'].values: continue
            
            c, v, h, l = df['Close'], df['Volume'], df['High'], df['Low']
            curr_p = float(c.iloc[-1])
            ma20, ma50, ma120 = c.rolling(20).mean().iloc[-1], c.rolling(50).mean().iloc[-1], c.rolling(120).mean().iloc[-1]
            
            # 指标计算
            adr = ((h - l) / l).rolling(20).mean().iloc[-1] * 100
            vol_ratio = v.iloc[-1] / v.iloc[-5:-1].mean()
            bias = ((curr_p - ma20) / ma20) * 100
            
            # 状态与共振 (使用紧凑符号)
            res = "3-Line" if curr_p > ma20 > ma50 > ma120 else "---"
            trend = "Up ↑" if curr_p > c.rolling(60).mean().iloc[-1] else "Side →"
            
            row = full_df[full_df['code'] == t].iloc[0]
            
            # Action 逻辑
            volat_5 = (c.iloc[-5:].std() / c.iloc[-5:].mean()) * 100
            if volat_5 < 2.2 and vol_ratio < 0.9: action = "🎯Setup"
            elif curr_p > h.iloc[-2] and vol_ratio > 1.4: action = "🚀Break"
            else: action = "Hold"

            # 最终打分
            score = row['Final_Rank'] + (15 if "Setup" in action else 0) + (10 if "3-Line" in res else 0)

            if row['Final_Rank'] > 70:
                results.append({
                    "Ticker": meta[t]['symbol'],
                    "Ind.": meta[t]['ind'],
                    "Score": int(score),
                    "1D%": f"{row['r1']*100:+.2f}%", # 强制显示符号
                    "Action": action,
                    "Reson.": res,
                    "60D Tr.": trend,
                    "Price": round(curr_p, 2),
                    "ADR": round(adr, 2),
                    "Vol.R": round(vol_ratio, 1),
                    "Bias": f"{bias:+.1f}%",
                    "Rank": int(row['Final_Rank']),
                    "REL5": int(row['REL5']),
                    "REL20": int(row['REL20']),
                    "REL60": int(row['REL60']),
                    "REL120": int(row['REL120']),
                    "R20": f"{curr_p/c.iloc[-20]:.2f}",
                    "R60": f"{curr_p/c.iloc[-60]:.2f}",
                    "R120": f"{curr_p/c.iloc[-120]:.2f}",
                    "MktCap": f"{meta[t]['mktcap']/1e8:.0f}Y",
                    "From241231": f"{row['rytd']*100:+.1f}%"
                })
        except: continue

    # 5. 写入与格式化
    sh = init_sheet()
    sh.clear()
    if results:
        # 排序并筛选
        df_final = pd.DataFrame(results).sort_values("Score", ascending=False).head(50)
        
        # 规范化列顺序 (逻辑分组：核心 -> 信号 -> 动力 -> 排名 -> 长期)
        cols = [
            "Ticker", "Ind.", "Score", "Action", "Price", "1D%", 
            "Reson.", "60D Tr.", "ADR", "Vol.R", "Bias", 
            "Rank", "REL5", "REL20", "REL60", "REL120", 
            "R20", "R60", "R120", "MktCap", "From241231"
        ]
        df_final = df_final[cols]
        
        # 写入
        sh.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        print(f"✅ 间距优化版同步完成。锁定 {len(df_final)} 只标的。")

if __name__ == "__main__":
    run_v53_optimizer()
