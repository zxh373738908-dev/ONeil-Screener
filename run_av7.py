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
# ==========================================

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client.open_by_key(SS_KEY).worksheet(TARGET_SHEET_NAME)

def get_market_data():
    """获取沪深300作为基准"""
    idx = yf.download("000300.SS", period="260d", progress=False)['Close']
    return idx

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 启动 V53.3 强势领涨股早鸟扫描...")

    # 1. 获取基准与股票池
    benchmark = get_market_data()
    
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "industry", "close", "market_cap_basic", "volume"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 100e8}], # 提高到100亿，过滤垃圾股
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 600]
    }
    raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    
    tickers = []
    meta = {}
    for item in raw_data:
        code = item['s'].split(':')[-1]
        yf_code = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        tickers.append(yf_code)
        meta[yf_code] = {"name": item['d'][1], "industry": item['d'][2], "symbol": code}

    # 2. 批量下载数据
    all_data = yf.download(tickers, period="260d", group_by='ticker', progress=True, threads=True)
    
    results = []
    rs_scores = []

    # 3. 第一遍循环：计算 RS 分数
    print("📊 正在计算 RS 评级与紧致度...")
    for yf_code in tickers:
        try:
            df = all_data[yf_code].dropna()
            if len(df) < 120: continue
            
            # RS 评分逻辑 (加权：1月*0.4 + 3月*0.2 + 6月*0.2 + 12月*0.2)
            c = df['Close']
            perf = (c.iloc[-1]/c.iloc[-22]*0.4) + (c.iloc[-1]/c.iloc[-66]*0.2) + \
                   (c.iloc[-1]/c.iloc[-132]*0.2) + (c.iloc[-1]/c.iloc[-250]*0.2)
            rs_scores.append({"code": yf_code, "score": perf})
        except: continue

    # 转换为百分位排名 (0-99)
    rs_df = pd.DataFrame(rs_scores)
    rs_df['rank'] = rs_df['score'].rank(pct=True) * 99

    # 4. 第二遍循环：精选强势形态
    for yf_code in tickers:
        try:
            df = all_data[yf_code].dropna()
            if len(df) < 50: continue
            
            close = df['Close']
            vol = df['Volume']
            
            # A. 核心指标计算
            curr_price = float(close.iloc[-1])
            ma50 = close.rolling(50).mean().iloc[-1]
            bias_50 = ((curr_price - ma50) / ma50) * 100
            
            # B. 紧致度 (最近10天收盘价的标准差/均值，越小越紧)
            tightness = (close.iloc[-10:].std() / close.iloc[-10:].mean()) * 100
            
            # C. 量比 (今日量 / 5日均量)
            vol_ratio = vol.iloc[-1] / vol.iloc[-6:-1].mean()
            
            # D. RS 线新高 (个股/指数 比值)
            rs_line = close / benchmark.reindex(close.index).ffill()
            is_rs_high = "是" if rs_line.iloc[-1] >= rs_line.iloc[-120:].max() else "否"
            
            # E. RS 评级
            this_rank = rs_df[rs_df['code'] == yf_code]['rank'].values[0]

            # --- 筛选逻辑 (早鸟发现核心) ---
            # 1. RS评级 > 85 (属于市场前15%的强势股)
            # 2. 价格在50日线上方 (bias_50 > -2) 且不太远
            # 3. 量比放大 (> 1.2)
            if this_rank > 80 and bias_50 > -5 and vol_ratio > 1.1:
                
                # 计算止损与目标 (ATR简易版)
                atr = (df['High'] - df['Low']).rolling(14).mean().iloc[-1]
                stop_loss = curr_price - (2 * atr)
                target = curr_price + (6 * atr)
                win_loss_ratio = round((target - curr_price) / (curr_price - stop_loss), 2)

                results.append({
                    "RS评级": int(this_rank),
                    "代码": meta[yf_code]['symbol'],
                    "名称": meta[yf_code]['name'],
                    "现价": round(curr_price, 2),
                    "量比": round(vol_ratio, 2),
                    "紧致度": f"{round(tightness, 2)}%",
                    "50日乖离": f"{round(bias_50, 1)}%",
                    "盈亏比": win_loss_ratio,
                    "RS线新高": is_rs_high,
                    "止损": round(stop_loss, 2),
                    "目标": round(target, 2),
                    "行业": meta[yf_code]['industry'],
                    "更新时间": now.strftime('%H:%M')
                })
        except: continue

    # 5. 排序并写入
    if results:
        df_final = pd.DataFrame(results).sort_values("RS评级", ascending=False)
        sh = init_sheet()
        sh.clear()
        sh.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        print(f"✅ 成功提取 {len(df_final)} 只强势早鸟股！")
    else:
        print("⚠️ 未发现符合条件的强势启动标的。")

if __name__ == "__main__":
    run_v53_optimizer()
