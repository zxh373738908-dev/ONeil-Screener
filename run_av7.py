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

# 早鸟核心阈值 (针对周三/盘后伏击)
RS_MIN = 75           # RS 评级前 25%
TIGHTNESS_MAX = 3.5   # 紧致度需小于 3.5% (波动越小爆发力越强)
DIST_MA20_MAX = 5.0   # 距离 20 日线不超过 5%，确保不是追高
# ==========================================

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client.open_by_key(SS_KEY).worksheet(TARGET_SHEET_NAME)

def get_benchmark():
    # 获取沪深300作为计算RS的基准
    idx = yf.download("000300.SS", period="260d", progress=False)['Close']
    return idx

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 启动 V53.3 高胜率早鸟伏击扫描...")

    benchmark = get_benchmark()
    
    # 1. 扩大池子到 800 只，确保覆盖到所有成长龙头
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "industry", "close", "market_cap_basic", "volume"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}], 
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 800]
    }
    raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    
    tickers, meta = [], {}
    for item in raw_data:
        code = item['s'].split(':')[-1]
        yf_code = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        tickers.append(yf_code)
        meta[yf_code] = {"name": item['d'][1], "industry": item['d'][2], "symbol": code}

    # 2. 批量下载
    print(f"📥 正在分析 {len(tickers)} 只标的...")
    all_data = yf.download(tickers, period="260d", group_by='ticker', progress=True, threads=True)
    
    rs_scores = []
    # 第一遍：计算 RS
    for yf_code in tickers:
        try:
            c = all_data[yf_code]['Close'].dropna()
            if len(c) < 120: continue
            # 欧奈尔 RS 权重计算
            perf = (c.iloc[-1]/c.iloc[-22]*0.4) + (c.iloc[-1]/c.iloc[-66]*0.2) + (c.iloc[-1]/c.iloc[-132]*0.2) + (c.iloc[-1]/c.iloc[-250]*0.2)
            rs_scores.append({"code": yf_code, "score": perf})
        except: continue
    
    rs_df = pd.DataFrame(rs_scores)
    rs_df['rank'] = rs_df['score'].rank(pct=True) * 99

    # 3. 第二遍：伏击形态识别
    results = []
    for yf_code in tickers:
        try:
            df = all_data[yf_code].dropna()
            if len(df) < 60: continue
            
            close = df['Close']
            curr_price = float(close.iloc[-1])
            
            # A. 均线与乖离
            ma20 = close.rolling(20).mean().iloc[-1]
            ma50 = close.rolling(50).mean().iloc[-1]
            bias_50 = ((curr_price - ma50) / ma50) * 100
            dist_ma20 = ((curr_price - ma20) / ma20) * 100
            
            # B. 紧致度 (过去 5 天波动率，核心伏击指标)
            tightness = (close.iloc[-5:].std() / close.iloc[-5:].mean()) * 100
            
            # C. 量能分析 (寻找缩量蓄势)
            vol_ratio = df['Volume'].iloc[-1] / df['Volume'].iloc[-10:-1].mean()
            
            # D. RS 线新高
            rs_line = close / benchmark.reindex(close.index).ffill()
            is_rs_high = "🔥新高" if rs_line.iloc[-1] >= rs_line.iloc[-60:].max() else "--"
            
            # E. 获取 RS 评级
            this_rank = rs_df[rs_df['code'] == yf_code]['rank'].values[0]

            # --- 高胜率早鸟过滤条件 ---
            # 1. 长期趋势向上 (Close > MA50)
            # 2. 属于强势股 (RS评级 > 75)
            # 3. 正在缩量蓄势 (量比 < 1.5 且 波动小)
            # 4. 价格回踩到位 (距离MA20很近)
            if this_rank > RS_MIN and bias_50 > -2 and dist_ma20 < DIST_MA20_MAX and tightness < TIGHTNESS_MAX:
                
                # 止损/目标计算
                atr = (df['High'] - df['Low']).rolling(14).mean().iloc[-1]
                stop_p = curr_price - (1.5 * atr)
                target_p = curr_price + (4 * atr)

                results.append({
                    "RS评级": int(this_rank),
                    "代码": meta[yf_code]['symbol'],
                    "名称": meta[yf_code]['name'],
                    "现价": round(curr_price, 2),
                    "紧致度": f"{round(tightness, 2)}%",
                    "量比": round(vol_ratio, 2),
                    "50日乖离": f"{round(bias_50, 1)}%",
                    "盈亏比": round((target_p - curr_price)/(curr_price - stop_p), 2),
                    "RS线新高": is_rs_high,
                    "止损": round(stop_p, 2),
                    "目标": round(target_p, 2),
                    "行业": meta[yf_code]['industry'],
                    "更新时间": now.strftime('%m-%d %H:%M')
                })
        except: continue

    # 4. 写入 Google Sheets
    sh = init_sheet()
    sh.clear()
    if results:
        df_res = pd.DataFrame(results).sort_values("RS评级", ascending=False)
        sh.update([df_res.columns.values.tolist()] + df_res.values.tolist())
        print(f"✅ 成功锁定 {len(df_res)} 只高胜率早鸟股！已更新至表格。")
    else:
        sh.update_acell("A1", f"最后扫描: {now.strftime('%H:%M:%S')} (未发现蓄势标的)")
        print("⚠️ 当前市场没有符合‘窄幅蓄势’的高强度个股。")

if __name__ == "__main__":
    run_v53_optimizer()
