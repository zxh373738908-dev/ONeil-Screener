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
START_DATE_REF = "2024-12-31" # 用于计算 From 2025-12-31 逻辑 (通常指今年以来)
# ==========================================

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client.open_by_key(SS_KEY).worksheet(TARGET_SHEET_NAME)

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 启动 V53.9 REL-Matrix 增强版...")

    # 1. 扫描池子
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "industry", "close", "market_cap_basic"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 85e8}], 
        "range": [0, 800]
    }
    raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    
    tickers, meta = [], {}
    for item in raw_data:
        code = item['s'].split(':')[-1]
        yf_code = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        tickers.append(yf_code)
        meta[yf_code] = {"name": item['d'][1], "industry": item['d'][2] or "Others", "symbol": code}

    # 2. 批量获取数据 (增加到 250d 以确保 120D 计算准确)
    all_data = yf.download(tickers, period="250d", group_by='ticker', progress=True, threads=True)
    
    # 3. 构建全样本强度矩阵 (REL 计算基础)
    stats = []
    for t in tickers:
        try:
            df = all_data[t].dropna()
            if len(df) < 125: continue
            c = df['Close']
            
            # 计算原始收益率 (用于排名)
            stats.append({
                "code": t,
                "ret_1d": (c.iloc[-1] / c.iloc[-2]) - 1,
                "ret_5d": (c.iloc[-1] / c.iloc[-5]) - 1,
                "ret_20d": (c.iloc[-1] / c.iloc[-20]) - 1,
                "ret_60d": (c.iloc[-1] / c.iloc[-60]) - 1,
                "ret_120d": (c.iloc[-1] / c.iloc[-120]) - 1,
                # 特定日期至今 (From 2024-12-31)
                "ret_custom": (c.iloc[-1] / c.loc[c.index >= START_DATE_REF].iloc[0]) - 1 if any(c.index >= START_DATE_REF) else 0
            })
        except: continue
    
    if not stats: return
    full_df = pd.DataFrame(stats)
    
    # 计算百分比排名 (Rank 0-99) -> 这就是 REL 系列
    for col in ['ret_5d', 'ret_20d', 'ret_60d', 'ret_120d']:
        rel_col = "REL" + col.split('_')[1].replace('d', '')
        full_df[rel_col] = full_df[col].rank(pct=True) * 99

    # 综合 Rank (基于 RS 权重)
    full_df['Final_Rank'] = (full_df['REL60'] * 0.4 + full_df['REL20'] * 0.4 + full_df['REL120'] * 0.2)

    # 4. 逻辑扫描与结果组装
    results = []
    for t in tickers:
        try:
            df = all_data[t].dropna()
            if len(df) < 60: continue
            c = df['Close']
            curr_p = float(c.iloc[-1])
            ma60 = c.rolling(60).mean().iloc[-1]
            
            # 提取该股在全样本中的 REL 数据
            row = full_df[full_df['code'] == t].iloc[0]
            
            # 计算 R20, R60, R120 (Price / Price_N)
            r20 = curr_p / c.iloc[-20]
            r60 = curr_p / c.iloc[-60]
            r120 = curr_p / c.iloc[-120]
            
            # 60-Day Trend 定义
            trend_60 = "📈上升" if curr_p > ma60 and c.iloc[-1] > c.iloc[-10] else "📉整理"
            
            # 基础过滤: Rank 较高 且 在 60日线附近或上方
            if row['Final_Rank'] > 70 and curr_p > ma60 * 0.95:
                results.append({
                    "代码": meta[t]['symbol'],
                    "名称": meta[t]['name'],
                    "Price": round(curr_p, 2),
                    "1D%": f"{round(row['ret_1d']*100, 2)}%",
                    "60-Day Trend": trend_60,
                    "R20": round(r20, 3),
                    "R60": round(r60, 3),
                    "R120": round(r120, 3),
                    "Rank": int(row['Final_Rank']),
                    "REL5": int(row['REL5']),
                    "REL20": int(row['REL20']),
                    "REL60": int(row['REL60']),
                    "REL120": int(row['REL120']),
                    "From 2025-01-01": f"{round(row['ret_custom']*100, 1)}%",
                    "行业": meta[t]['industry'],
                    "更新": now.strftime('%H:%M')
                })
        except: continue

    # 5. 写入 Google Sheets
    sh = init_sheet()
    sh.clear()
    if results:
        df_final = pd.DataFrame(results).sort_values("Rank", ascending=False).head(50)
        # 强制按用户要求的顺序排列列
        final_cols = ["代码", "名称", "Price", "1D%", "60-Day Trend", "R20", "R60", "R120", "Rank", "REL5", "REL20", "REL60", "REL120", "From 2025-01-01", "行业", "更新"]
        df_final = df_final[final_cols]
        
        sh.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        print(f"✅ 成功！已根据 REL 矩阵更新 {len(df_final)} 只强庄股。")
    else:
        print("⚠️ 未发现匹配标的。")

if __name__ == "__main__":
    run_v53_optimizer()
