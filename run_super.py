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

def get_universe():
    try:
        print("📡 正在獲取全市場候選股...")
        ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        req = urllib.request.Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': ua})
        sp500 = pd.read_html(urllib.request.urlopen(req).read())[0]['Symbol'].tolist()
        req2 = urllib.request.Request('https://en.wikipedia.org/wiki/Nasdaq-100', headers={'User-Agent': ua})
        ndx = pd.read_html(urllib.request.urlopen(req2).read())[4]['Ticker'].tolist()
        return list(set([t.replace('.', '-') for t in sp500 + ndx]))
    except:
        return ["AAPL", "MSFT", "GOOGL", "AVGO", "CAVA", "VRT", "MPWR", "NVDA", "LITE", "TER", "KEYS", "MRVL"]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'REIT', 'Utilities', 'Oil & Gas']

# ==========================================
# 2. 核心避錯函數
# ==========================================
def fetch_info(t):
    """降低併發頻率，減少 401 錯誤"""
    for i in range(3):
        try:
            time.sleep(random.uniform(0.5, 1.5)) # 增加延遲避開監測
            ticker = yf.Ticker(t)
            info = ticker.info
            if info and 'industry' in info:
                return t, info
        except:
            time.sleep(2)
    return t, {}

def sync_to_google_sheet(sheet_name, matrix):
    try:
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=lambda x: str(x) if isinstance(x, (datetime.date, datetime.datetime)) else x))}
        requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"🎉 同步成功至分頁: [{sheet_name}]")
    except Exception as e: print(f"❌ 同步失敗: {e}")

def get_return(series, days):
    s = series.dropna()
    if len(s) < days + 1: return 0
    return (float(s.iloc[-1]) - float(s.iloc[-(days+1)])) / float(s.iloc[-(days+1)])

# ==========================================
# 3. 核心量化模型 V11.2
# ==========================================
def run_super_growth_v11():
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    UNIVERSE = get_universe()
    
    print("\n" + "="*50)
    print(f"🚀 [超級成長股 V11.2] 正在執行精準掃描...")
    
    # 1. 指標獲取
    try:
        spy = yf.download("SPY", period="1y", interval="1d", progress=False)['Close'].dropna()
        vix = float(yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1])
        curr_spy, ma50_spy = float(spy.iloc[-1]), float(spy.tail(50).mean())
        spy_ret = {20: get_return(spy, 20), 60: get_return(spy, 60)}
        weather = "☀️" if curr_spy > ma50_spy and vix < 20 else ("☁️" if curr_spy > ma50_spy or vix < 25 else "⛈️")
    except: weather, vix, spy_ret = "❓", 0, {20:0, 60:0}

    # 2. 技術面
    hist = yf.download(UNIVERSE, period="1y", interval="1d", progress=False, threads=True)
    close, vol, high, low = hist['Close'], hist['Volume'], hist['High'], hist['Low']

    tech_results, perfect_tickers = {}, []
    for t in UNIVERSE:
        try:
            if t not in close.columns: continue
            c, v, h, l = close[t].dropna(), vol[t].dropna(), high[t].dropna(), low[t].dropna()
            if len(c) < 150: continue
            p = float(c.iloc[-1])
            m20, m50, m200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            
            if p > m20 > m50 > m200: perfect_tickers.append(t)
            if not (p > m50 and m50 > m200): continue
            if v.tail(40).mean() * p < 20_000_000: continue
            
            rs_score = (get_return(c, 20) * 0.4) + (get_return(c, 60) * 0.3) + (get_return(c, 120) * 0.3)
            tech_results[t] = {
                "Price": p, "ADR": ((h - l) / l).tail(20).mean() * 100,
                "VolRatio": v.iloc[-1] / v.tail(20).mean() if v.tail(20).mean() > 0 else 1,
                "Bias": ((p - m20) / m20) * 100, "RS_Raw": rs_score,
                "5D": get_return(c, 5), "20D": get_return(c, 20), "60D": get_return(c, 60),
                "R20": get_return(c, 20) - spy_ret[20], "R60": get_return(c, 60) - spy_ret[60]
            }
        except: continue

    # 3. 基本面 (降低併發數減少 401)
    print(f"✅ 行情共振: {len(perfect_tickers)} 隻 | 正在獲取基本面 (慢速模式)...")
    infos = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        for t, info in executor.map(fetch_info, list(tech_results.keys())):
            if info: infos[t] = info

    res_map = {}
    for t in perfect_tickers:
        if t in infos:
            ind = infos[t].get('industry', 'Unknown')
            res_map[ind] = res_map.get(ind, 0) + 1

    final_candidates = []
    rs_ranks = (pd.Series({t: d['RS_Raw'] for t, d in tech_results.items()}).rank(pct=True) * 100).to_dict()
    
    for t, data in tech_results.items():
        if t not in infos: continue
        info = infos[t]
        industry = info.get('industry', 'Unknown')
        if any(ex.lower() in industry.lower() for ex in EXCLUDED_INDUSTRIES): continue
        
        rs = rs_ranks.get(t, 0)
        score = (rs * 0.7) + ((info.get('revenueGrowth', 0) or 0) * 100 * 0.3)
        
        # 智慧 Action / Options
        action = "🎯 買點區" if data['Bias'] < 5 and rs > 90 else ("⌛ 回踩" if data['Bias'] > 12 else "👀 觀察")
        opt = "🔥 Call" if data['ADR'] > 3.5 and rs > 90 else "N/A"

        final_candidates.append({
            "Ticker": t, "Industry": industry[:18], "Score": score, "Action": action,
            "Resonance": res_map.get(industry, 0), "ADR": data['ADR'], "Vol": data['VolRatio'],
            "Bias": data['Bias'], "MCap": info.get('marketCap', 0) / 1_000_000, "RS": rs,
            "Opt": opt, "Price": data['Price'], "5D": data['5D'], "20D": data['20D'],
            "60D": data['60D'], "R20": data['R20'], "R60": data['R60']
        })

    # 優先選取評分最高的前 10 隻
    top_10 = sorted(final_candidates, key=lambda x: x['Score'], reverse=True)[:10]

    # ==========================================
    # 4. 17列精確矩陣輸出
    # ==========================================
    m_text = f"天气:{weather} | 宽度:{(len(perfect_tickers)/len(UNIVERSE)*100):.1f}% | 共振:{len(perfect_tickers)}隻 | VIX:{vix:.1f}"
    row1 = [f"SuperGrowth Portfolio V11.2", f"更新: {update_time}", m_text, ""] + [""] * 13
    row2 = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap(M)", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
    
    matrix = [row1, row2]
    for r in top_10:
        matrix.append([
            r['Ticker'], r['Industry'], round(r['Score'], 1), r['Action'], f"{r['Resonance']} 隻",
            f"{round(r['ADR'], 2)}%", f"{round(r['Vol'], 2)}x", f"{round(r['Bias'], 2)}%",
            round(r['MCap'], 1), round(r['RS'], 1), r['Opt'],
            float(r['Price']), # 輸出純數字，防止百分比格式化錯誤
            f"{round(r['5D']*100, 2)}%", f"{round(r['20D']*100, 2)}%", f"{round(r['60D']*100, 2)}%",
            f"{round(r['R20']*100, 2)}%", f"{round(r['R60']*100, 2)}%"
        ])

    sync_to_google_sheet(TARGET_SHEET, matrix)

if __name__ == "__main__":
    run_super_growth_v11()
