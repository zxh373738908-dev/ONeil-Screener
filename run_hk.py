import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import yfinance as yf
import requests
import re
import time
from gspread_formatting import *

warnings.filterwarnings('ignore')

# ==========================================
# 1. 核心配置
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_GID = 665566258  
CREDS_FILE = "credentials.json"
ACCOUNT_SIZE = 1000000 
MAX_RISK_PER_TRADE = 0.008 # 严格控制：单笔风险 0.8%

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    for ws in doc.worksheets():
        if str(ws.id) == str(TARGET_GID): return ws
    return doc.get_worksheet(0)

# ==========================================
# 🧠 2. 量子哨兵引擎 (Flagship V750)
# ==========================================
def calculate_quantum_sentinel(df, hsi_series, hstech_status):
    try:
        df = df.dropna(subset=['Close'])
        if len(df) < 250: return None
        
        close = df['Close'].values.astype(float)
        high = df['High'].values.astype(float)
        low = df['Low'].values.astype(float)
        vol = df['Volume'].values.astype(float)
        cp = close[-1]
        
        # 1. 流动性与均线
        avg_turnover_20d = np.mean((close * vol)[-20:])
        if avg_turnover_20d < 120000000: return None # 提升至1.2亿门槛

        ma50 = np.mean(close[-50:])
        ma200 = np.mean(close[-200:])
        dist_ma50 = (cp / ma50 - 1) * 100 # 乖离率

        # 2. RS 加速检测
        hsi_val = hsi_series.reindex(df.index).ffill().values
        rs_line = close / hsi_val
        rs_slope = (rs_line[-1] / rs_line[-6] - 1) * 100 # 近一周RS斜率
        rs_nh = rs_line[-1] >= np.max(rs_line[-252:])

        # 3. 筹码位与紧致度
        price_bins = np.linspace(np.min(low[-100:]), np.max(high[-100:]), 40)
        hist, edges = np.histogram(close[-100:], bins=price_bins, weights=vol[-100:])
        poc_price = edges[np.argmax(hist)]
        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100
        
        # 4. 口袋枢轴逻辑
        neg_vol = vol[-11:-1][close[-11:-1] < close[-12:-2]]
        max_neg_vol = np.max(neg_vol) if len(neg_vol) > 0 else 9e12
        is_pocket = (close[-1] > close[-2]) and (vol[-1] > max_neg_vol)

        # 5. 战法判定 (加入乖离率惩罚)
        action = "观察"
        score = 65
        if dist_ma50 > 15: # 严重偏离，警告风险
            action, score = "⚠️ 乖離過大", 40
        elif is_pocket and cp > poc_price and tightness < 1.6:
            action, score = "🎯 領袖口袋(Pocket)", 95
        elif rs_nh and cp >= np.max(close[-20:]) and tightness < 2.0:
            action, score = "🚀 巔峰突破(Breakout)", 90
        elif cp > ma50 and tightness < 1.0 and vol[-1] < np.mean(vol[-20:])*0.5:
            action, score = "💎 极致收缩(VCP)", 85

        # 环境加成
        if not hstech_status: score -= 15 # 指数不好，降级处理

        # 6. 风险与头寸
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
        stop_price = max(ma50 * 0.985, cp * (1 - adr_20 * 0.01 * 1.5))
        risk_per_share = cp - stop_price
        shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // risk_per_share if risk_per_share > 0 else 0

        return {
            "Action": action, "Score": score, "Price": cp, "Dist_MA50": round(dist_ma50, 1),
            "Shares": int(shares), "Stop": round(stop_price, 2), "Tight": round(tightness, 2),
            "Turnover_M": round(avg_turnover_20d / 1000000, 1), "RS_Turbo": round(rs_slope, 2),
            "Above_POC": "✅" if cp > poc_price else "-", "RS_NH": "🌟" if rs_nh else ""
        }
    except: return None

# ==========================================
# 🚀 3. 执行主逻辑
# ==========================================
def main():
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V45 量子哨兵旗舰版启动...")
    
    # 1. 环境审计 (HSI & HSTECH)
    mkt_data = yf.download(["^HSI", "3088.HK"], period="50d", progress=False)['Close']
    hsi_series = mkt_data["^HSI"].dropna()
    hstech = mkt_data["3088.HK"].dropna()
    hstech_ok = hstech.iloc[-1] > hstech.rolling(20).mean().iloc[-1]
    mkt_weather = "☀️ 激进" if hstech_ok else "☁️ 谨慎"

    # 2. 票池初筛 (TradingView)
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 2e10}],
               "range": [0, 300], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        resp = requests.post(url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "其他"} for d in resp])
        tickers = [c.zfill(4)+".HK" for c in df_pool['code']]
    except: return

    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    final_list = []
    
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            res = calculate_quantum_sentinel(data[t], hsi_series, hstech_ok)
            if res and res['Action'] != "观察":
                res.update({"Ticker": t.split('.')[0], "Sector": df_pool[df_pool['code']==t.split('.')[0].lstrip('0')].iloc[0]['sector']})
                final_list.append(res)
        except: continue

    if not final_list: return
    res_df = pd.DataFrame(final_list)
    
    # 行业配额筛选：每个行业只取前 3 名
    res_df = res_df.sort_values(by="Score", ascending=False).groupby("Sector").head(3)
    
    # 写入 Google Sheets
    sh = init_sheet()
    sh.clear()
    sh.update(range_name="A1", values=[[f"🏯 V45 量子哨兵旗舰版", f"环境天气: {mkt_weather}", f"刷新: {now_str}", "策略: 乖离率修正 + 行业配额 + 机构口袋"]])
    
    cols = ["Ticker", "Action", "Score", "Price", "Shares", "Stop", "Tight", "Dist_MA50", "RS_Turbo", "Sector"]
    sh.update(range_name="A3", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")

    # 4. 极致美化
    set_frozen(sh, rows=3)
    format_cell_range(sh, 'B4:B100', cellFormat(textFormat=textFormat(bold=True)))
    # 乖离率风险警告：黄色底
    rules = get_conditional_format_rules(sh)
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('H4:H100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['12']),
                                format=cellFormat(backgroundColor=color(1, 0.9, 0.7)))))
    rules.save()
    print(f"✅ 任务完成。当前环境：{mkt_weather}，发现 {len(res_df)} 个加固信号。")

if __name__ == "__main__":
    main()
