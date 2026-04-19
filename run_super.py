import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import requests
import json
import warnings
import math
import urllib.request
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

# ==========================================
# 1. 系統配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycby1pIM7iO43lcLQpOmi5LCJIn3VN9a0Ilf9amoy1EtQV_GBXJkk_A4PpsrJxKzH7i51/exec"
TARGET_SHEET = "super"
START_DATE_YTD = "2025-12-31"

def get_universe():
    try:
        ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        req = urllib.request.Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': ua})
        sp500 = pd.read_html(urllib.request.urlopen(req).read())[0]['Symbol'].tolist()
        return [t.replace('.', '-') for t in sp500]
    except:
        return ["AAPL", "MSFT", "GOOGL", "AVGO", "CAVA", "VRT", "MPWR", "NVDA", "LITE", "TER", "KEYS", "MRVL"]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'REIT', 'Utilities', 'Oil & Gas']

# ==========================================
# 2. 核心數據處理
# ==========================================
def fetch_info_v15(t):
    for i in range(3):
        try:
            time.sleep(random.uniform(0.5, 0.8)) 
            ticker = yf.Ticker(t)
            info = ticker.info
            if info and ('industry' in info): return t, info
        except: time.sleep(1.5)
    return t, {}

def sync_to_google_sheet(sheet_name, matrix):
    try:
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=str))}
        requests.post(WEBAPP_URL, json=payload, timeout=40)
        print(f"🎉 V15 同步成功！")
    except Exception as e: print(f"❌ 同步失敗: {e}")

def get_ret(series, days):
    if len(series) < days + 1: return 0
    return (series.iloc[-1] / series.iloc[-(days+1)]) - 1

# ==========================================
# 3. 核心量化模型 V15
# ==========================================
def run_super_growth_v15():
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    tickers = get_universe()
    
    print("\n" + "="*50)
    print(f"🚀 [超級成長股 V15] 啟動 | YTD起始日: {START_DATE_YTD}")
    
    # 1. 基準與大盤下載
    try:
        spy = yf.download("SPY", start="2025-01-01", progress=False)['Close'].dropna()
        vix = float(yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1])
        curr_spy, ma50_spy = spy.iloc[-1], spy.tail(50).mean()
        
        # 基準報酬
        spy_r = {5: get_ret(spy, 5), 20: get_ret(spy, 20), 60: get_ret(spy, 60), 120: get_ret(spy, 120)}
        weather = "☀️" if curr_spy > ma50_spy and vix < 20 else ("☁️" if curr_spy > ma50_spy or vix < 25 else "⛈️")
    except: weather, vix, spy_r = "❓", 0, {5:0, 20:0, 60:0, 120:0}

    # 2. 全市場技術掃描
    hist = yf.download(tickers, start="2025-01-01", progress=False, threads=True)
    close_df = hist['Close']

    tech_results, above_50ma, perfect_tickers = {}, 0, []

    for t in tickers:
        try:
            if t not in close_df.columns: continue
            c = close_df[t].dropna()
            if len(c) < 150: continue
            
            p = float(c.iloc[-1])
            m20, m50, m200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            
            if p > m50: above_50ma += 1
            if p > m20 > m50 > m200: perfect_tickers.append(t)
            
            # 過濾門檻
            if not (p > m50 and m50 > m200): continue
            
            # YTD 報酬計算 (從 2025-12-31 到現在)
            # 如果數據從 2025-01-01 開始，我們找最近的一個價格
            ytd_price = c.iloc[0] 
            ytd_ret = (p / ytd_price) - 1
            
            # 趨勢簡化符號 (60-Day Trend)
            trend_60 = "📈" if p > c.tail(60).mean() else "📉"
            
            rs_score = (get_ret(c, 20) * 0.4) + (get_ret(c, 60) * 0.3) + (get_ret(c, 120) * 0.3)
            
            tech_results[t] = {
                "Price": p, "1D": (c.iloc[-1]/c.iloc[-2])-1, "Trend": trend_60,
                "R20": get_ret(c, 20), "R60": get_ret(c, 60), "R120": get_ret(c, 120),
                "REL5": get_ret(c, 5) - spy_r[5], "REL20": get_ret(c, 20) - spy_r[20],
                "REL60": get_ret(c, 60) - spy_r[60], "REL120": get_ret(c, 120) - spy_r[120],
                "YTD": ytd_ret, "RS_Raw": rs_score
            }
        except: continue

    us_breadth = (above_50ma / len(tickers) * 100)

    # 3. 基本面
    infos = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        for t, info in executor.map(fetch_info_v15, list(tech_results.keys())):
            if info: infos[t] = info

    res_map = {}
    for t in perfect_tickers:
        ind = infos.get(t, {}).get('industry', 'Unknown')
        res_map[ind] = res_map.get(ind, 0) + 1

    # 4. 排名與構建矩陣
    rs_ranks = (pd.Series({t: d['RS_Raw'] for t, d in tech_results.items()}).rank(pct=True) * 100).to_dict()
    
    final_list = []
    for t, data in tech_results.items():
        info = infos.get(t, {})
        industry = info.get('industry', 'Data Missing')
        if any(ex.lower() in industry.lower() for ex in EXCLUDED_INDUSTRIES): continue
        
        rank = rs_ranks.get(t, 0)
        score = (rank * 0.7) + ((info.get('revenueGrowth', 0) or 0) * 100 * 0.3)

        final_list.append([
            t, industry[:16], 
            round(data['Price'], 2), 
            f"{round(data['1D']*100, 2)}%", 
            data['Trend'],
            f"{round(data['R20']*100, 2)}%", 
            f"{round(data['R60']*100, 2)}%", 
            f"{round(data['R120']*100, 2)}%", 
            round(rank, 1),
            f"{round(data['REL5']*100, 2)}%",
            f"{round(data['REL20']*100, 2)}%", 
            f"{round(data['REL60']*100, 2)}%", 
            f"{round(data['REL120']*100, 2)}%",
            f"{round(data['YTD']*100, 2)}%",
            round(info.get('marketCap', 0)/1000000, 1),
            f"{res_map.get(industry, 0)} 隻",
            round(score, 1)
        ])

    top_10 = sorted(final_list, key=lambda x: x[-1], reverse=True)[:10]

    # ==========================================
    # 5. 17列精確對齊輸出
    # ==========================================
    header_text = f"天气:{weather} | 全美宽度(50MA):{us_breadth:.1f}% | 完美共振:{len(perfect_tickers)}隻 | VIX:{vix:.1f}"
    row1 = [f"SuperGrowth Portfolio V15", f"更新: {update_time}", header_text, ""] + [""] * 13
    row2 = ["Ticker", "Industry", "Price", "1D%", "60-Day Trend", "R20", "R60", "R120", "Rank", "REL5", "REL20", "REL60", "REL120", "From 2025-12-31", "MktCap(M)", "Resonance", "Score"]
    
    sync_to_google_sheet(TARGET_SHEET, [row1, row2] + top_10)

if __name__ == "__main__":
    run_super_growth_v15()
