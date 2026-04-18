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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
]

def get_universe():
    try:
        ua = random.choice(USER_AGENTS)
        req = urllib.request.Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': ua})
        sp500 = pd.read_html(urllib.request.urlopen(req).read())[0]['Symbol'].tolist()
        return [t.replace('.', '-') for t in sp500]
    except:
        return ["AAPL", "MSFT", "GOOGL", "AVGO", "CAVA", "VRT", "MPWR", "NVDA", "LITE", "TER", "KEYS", "MRVL"]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'REIT', 'Utilities', 'Oil & Gas']

# ==========================================
# 2. 強化版數據抓取
# ==========================================
def fetch_stock_data(t):
    """具備隨機 UA 和軟著陸的基本面抓取"""
    ticker = yf.Ticker(t)
    info = {}
    try:
        time.sleep(random.uniform(0.5, 1.2)) # 慢速抓取防止 401
        info = ticker.info
    except:
        # 軟著陸：如果 info 被封，嘗試獲取 fast_info (不容易被封)
        try:
            fast = ticker.fast_info
            info = {
                'industry': 'Data Missing',
                'marketCap': fast.get('market_cap', 0),
                'revenueGrowth': 0 # 缺失則假設為0
            }
        except: pass
    return t, info

def sync_to_google_sheet(sheet_name, matrix):
    try:
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=str))}
        requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"🎉 同步成功至分頁: [{sheet_name}]")
    except Exception as e: print(f"❌ 同步失敗: {e}")

def get_return(series, days):
    s = series.dropna()
    if len(s) < days + 1: return 0
    return (float(s.iloc[-1]) - float(s.iloc[-(days+1)])) / float(s.iloc[-(days+1)])

# ==========================================
# 3. 核心量化模型 V12
# ==========================================
def run_super_growth_v12():
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    UNIVERSE = get_universe()
    
    print("\n" + "="*50)
    print(f"🚀 [超級成長股 V12] 啟動 | 正在使用模擬指紋避開 401...")
    
    # 1. 市場指標
    try:
        spy_hist = yf.download("SPY", period="1y", interval="1d", progress=False)['Close'].dropna()
        vix = float(yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1])
        curr_spy, ma50_spy = spy_hist.iloc[-1], spy_hist.tail(50).mean()
        spy_ret = {20: get_return(spy_hist, 20), 60: get_return(spy_hist, 60)}
        weather = "☀️" if curr_spy > ma50_spy and vix < 20 else ("☁️" if curr_spy > ma50_spy or vix < 25 else "⛈️")
    except: weather, vix, spy_ret = "❓", 0, {20:0, 60:0}

    # 2. 技術面掃描
    hist = yf.download(UNIVERSE, period="1y", interval="1d", progress=False, threads=True)
    close, vol, high, low = hist['Close'], hist['Volume'], hist['High'], hist['Low']

    tech_results, perfect_tickers = {}, []
    for t in UNIVERSE:
        try:
            if t not in close.columns: continue
            c, v, h, l_ = close[t].dropna(), vol[t].dropna(), high[t].dropna(), low[t].dropna()
            if len(c) < 150: continue
            p = float(c.iloc[-1])
            m20, m50, m200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            
            if p > m20 > m50 > m200: perfect_tickers.append(t)
            if not (p > m50 and m50 > m200): continue
            
            rs_raw = (get_return(c, 20) * 0.4) + (get_return(c, 60) * 0.3) + (get_return(c, 120) * 0.3)
            tech_results[t] = {
                "Price": p, "ADR": ((h - l_) / l_).tail(20).mean() * 100,
                "VolRatio": v.iloc[-1] / v.tail(20).mean() if v.tail(20).mean() > 0 else 1,
                "Bias": ((p - m20) / m20) * 100, "RS_Raw": rs_raw,
                "5D": get_return(c, 5), "20D": get_return(c, 20), "60D": get_return(c, 60),
                "R20": get_return(c, 20) - spy_ret[20], "R60": get_return(c, 60) - spy_ret[60]
            }
        except: continue

    # 3. 基本面獲取 (慢速穩定模式)
    print(f"✅ 行情共振: {len(perfect_tickers)} 隻 | 正在獲取基本面 (避錯模式)...")
    infos = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        for t, info in executor.map(fetch_stock_data, list(tech_results.keys())):
            if info: infos[t] = info

    res_map = {}
    for t in perfect_tickers:
        if t in infos:
            ind = infos[t].get('industry', 'Unknown')
            res_map[ind] = res_map.get(ind, 0) + 1

    final_pool = []
    rs_ranks = (pd.Series({t: d['RS_Raw'] for t, d in tech_results.items()}).rank(pct=True) * 100).to_dict()
    
    for t, data in tech_results.items():
        if t not in infos: continue
        info = infos[t]
        ind = info.get('industry', 'Unknown')
        if any(ex.lower() in ind.lower() for ex in EXCLUDED_INDUSTRIES): continue
        
        rs = rs_ranks.get(t, 0)
        score = (rs * 0.7) + ((info.get('revenueGrowth', 0) or 0) * 100 * 0.3)
        
        # 智慧決策
        action = "🎯 買點區" if data['Bias'] < 5 and rs > 90 else ("⌛ 等待回踩" if data['Bias'] > 12 else "👀 觀察")
        opt = "🔥 Call" if data['ADR'] > 3.5 and rs > 90 else "N/A"

        final_pool.append({
            "Ticker": t, "Industry": ind[:18], "Score": score, "Action": action,
            "Resonance": f"{res_map.get(ind, 0)} 隻", "ADR": f"{round(data['ADR'], 2)}%",
            "Vol": f"{round(data['VolRatio'], 2)}x", "Bias": f"{round(data['Bias'], 2)}%",
            "MCap": round(info.get('marketCap', 0) / 1000000, 1), "RS": round(rs, 1),
            "Opt": opt, "Price": data['Price'], "5D": f"{round(data['5D']*100, 2)}%",
            "20D": f"{round(data['20D']*100, 2)}%", "60D": f"{round(data['60D']*100, 2)}%",
            "R20": f"{round(data['R20']*100, 2)}%", "R60": f"{round(data['R60']*100, 2)}%"
        })

    # 排序：優先選取評分 Score 最高的前 10 隻
    top_10 = sorted(final_pool, key=lambda x: x['Score'], reverse=True)[:10]

    # ==========================================
    # 4. 輸出至 Google Sheets
    # ==========================================
    m_text = f"天气:{weather} | 宽度:{(len(perfect_tickers)/len(UNIVERSE)*100):.1f}% | 共振:{len(perfect_tickers)}隻 | VIX:{vix:.1f}"
    row1 = [f"SuperGrowth Portfolio V12", f"更新: {update_time}", m_text, ""] + [""] * 13
    row2 = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap(M)", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
    
    matrix = [row1, row2]
    for r in top_10:
        matrix.append([
            r['Ticker'], r['Industry'], round(r['Score'], 1), r['Action'], r['Resonance'],
            r['ADR'], r['Vol'], r['Bias'], r['MCap'], r['RS'], r['Opt'],
            float(r['Price']), # Price 傳數字，其餘帶 % 的傳字串
            r['5D'], r['20D'], r['60D'], r['R20'], r['R60']
        ])

    sync_to_google_sheet(TARGET_SHEET, matrix)

if __name__ == "__main__":
    run_super_growth_v12()
