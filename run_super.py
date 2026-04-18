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
        ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        req = urllib.request.Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': ua})
        sp500 = pd.read_html(urllib.request.urlopen(req).read())[0]['Symbol'].tolist()
        return [t.replace('.', '-') for t in sp500]
    except:
        return ["AAPL", "MSFT", "GOOGL", "AVGO", "CAVA", "VRT", "MPWR", "NVDA", "LITE", "TER", "KEYS", "MRVL"]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'REIT', 'Utilities', 'Oil & Gas']

# ==========================================
# 2. 核心數據獲取 (解決 401 報錯)
# ==========================================
def fetch_info_v13(t):
    for i in range(3):
        try:
            time.sleep(random.uniform(0.6, 1.2)) # 慢速抓取
            ticker = yf.Ticker(t)
            info = ticker.info
            if info and 'industry' in info:
                return t, info
        except:
            time.sleep(2)
    return t, {}

def sync_to_google_sheet(sheet_name, matrix):
    try:
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=str))}
        requests.post(WEBAPP_URL, json=payload, timeout=35)
        print(f"🎉 同步成功至分頁: [{sheet_name}]")
    except Exception as e: print(f"❌ 同步失敗: {e}")

def get_return(series, days):
    s = series.dropna()
    if len(s) < days + 1: return 0
    return (float(s.iloc[-1]) - float(s.iloc[-(days+1)])) / float(s.iloc[-(days+1)])

# ==========================================
# 3. 核心量化模型 V13
# ==========================================
def run_super_growth_v13():
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    universe_tickers = get_universe()
    
    print("\n" + "="*50)
    print(f"🚀 [超級成長股 V13] 正在執行全美寬度(50MA)計算...")
    
    # 1. 大盤指標
    try:
        spy_hist = yf.download("SPY", period="1y", interval="1d", progress=False)['Close'].dropna()
        vix_val = float(yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1])
        curr_spy, ma50_spy = float(spy_hist.iloc[-1]), float(spy_hist.tail(50).mean())
        spy_ret = {20: get_return(spy_hist, 20), 60: get_return(spy_hist, 60)}
        weather_icon = "☀️" if curr_spy > ma50_spy and vix_val < 20 else ("☁️" if curr_spy > ma50_spy or vix_val < 25 else "⛈️")
    except:
        weather_icon, vix_val, spy_ret = "❓", 0, {20:0, 60:0}

    # 2. 批量技術掃描
    hist = yf.download(universe_tickers, period="1y", interval="1d", progress=False, threads=True)
    close_df, vol_df, high_df, low_df = hist['Close'], hist['Volume'], hist['High'], hist['Low']

    tech_results, above_50ma_count, perfect_tickers = {}, 0, []

    for t in universe_tickers:
        try:
            if t not in close_df.columns: continue
            c, v, h, l_ = close_df[t].dropna(), vol_df[t].dropna(), high_df[t].dropna(), low_df[t].dropna()
            if len(c) < 150: continue
            
            p = float(c.iloc[-1])
            m20, m50, m200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            
            # --- 寬度算法修正 ---
            if p > m50: above_50ma_count += 1 # 這是你要求的全美寬度定義
            
            is_perfect = p > m20 > m50 > m200
            if is_perfect: perfect_tickers.append(t)
            
            if not (p > m50 and m50 > m200): continue
            if v.tail(40).mean() * p < 20_000_000: continue
            
            rs_raw = (get_return(c, 20) * 0.4) + (get_return(c, 60) * 0.3) + (get_return(c, 120) * 0.3)
            tech_results[t] = {
                "Price": p, "ADR": ((h - l_) / l_).tail(20).mean() * 100,
                "VolRatio": v.iloc[-1] / v.tail(20).mean() if v.tail(20).mean() > 0 else 1,
                "Bias": ((p - m20) / m20) * 100, "RS_Raw": rs_raw,
                "5D": get_return(c, 5), "20D": get_return(c, 20), "60D": get_return(c, 60),
                "R20": get_return(c, 20) - spy_ret[20], "R60": get_return(c, 60) - spy_ret[60],
                "Trend": "完美多頭" if is_perfect else "上升通道"
            }
        except: continue

    us_breadth = (above_50ma_count / len(universe_tickers) * 100)

    # 3. 基本面
    print(f"✅ 全美寬度(50MA): {us_breadth:.1f}% | 完美共振股: {len(perfect_tickers)} 隻")
    infos = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        for t, info in executor.map(fetch_info_v13, list(tech_results.keys())):
            if info: infos[t] = info

    res_map = {}
    for t in perfect_tickers:
        ind = infos.get(t, {}).get('industry', 'Unknown')
        res_map[ind] = res_map.get(ind, 0) + 1

    final_pool = []
    rs_ranks = (pd.Series({t: d['RS_Raw'] for t, d in tech_results.items()}).rank(pct=True) * 100).to_dict()
    
    for t, data in tech_results.items():
        info = infos.get(t, {})
        industry = info.get('industry', 'Data Missing')
        if any(ex.lower() in industry.lower() for ex in EXCLUDED_INDUSTRIES): continue
        
        rs = rs_ranks.get(t, 0)
        score = (rs * 0.7) + ((info.get('revenueGrowth', 0) or 0) * 100 * 0.3)
        
        action = "🎯 買點區" if data['Bias'] < 4 and rs > 90 else ("⌛ 等待回踩" if data['Bias'] > 12 else "👀 觀察")
        opt = "🔥 Call" if data['ADR'] > 3.5 and rs > 90 else "N/A"

        final_pool.append({
            "Ticker": t, "Industry": industry[:18], "Score": score, "Action": action,
            "Resonance": f"{res_map.get(industry, 0)} 隻", "ADR": f"{round(data['ADR'], 2)}%",
            "Vol": f"{round(data['VolRatio'], 2)}x", "Bias": f"{round(data['Bias'], 2)}%",
            "MCap": round(info.get('marketCap', 0) / 1000000, 1), "RS": round(rs, 1),
            "Opt": opt, "Price": data['Price'], "5D": f"{round(data['5D']*100, 2)}%",
            "20D": f"{round(data['20D']*100, 2)}%", "60D": f"{round(data['60D']*100, 2)}%",
            "R20": f"{round(data['R20']*100, 2)}%", "R60": f"{round(data['R60']*100, 2)}%"
        })

    # --- 關鍵修正：優先選擇 Score (動能與基本面) 最強的前 10 隻 ---
    top_10 = sorted(final_pool, key=lambda x: x['Score'], reverse=True)[:10]

    # ==========================================
    # 4. 17列矩陣精確同步
    # ==========================================
    h_text = f"天气:{weather_icon} | 全美宽度(50MA):{us_breadth:.1f}% | 行业共振:{len(perfect_tickers)}隻 | VIX:{vix_val:.1f}"
    row1 = [f"SuperGrowth Portfolio V13", f"更新: {update_time}", h_text, ""] + [""] * 13
    row2 = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap(M)", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
    
    matrix = [row1, row2]
    for r in top_10:
        matrix.append([
            r['Ticker'], r['Industry'], round(r['Score'], 1), r['Action'], r['Resonance'],
            r['ADR'], r['Vol'], r['Bias'], r['MCap'], r['RS'], r['Opt'],
            float(r['Price']), # L列格式請在Sheets手動設為「數值」
            r['5D'], r['20D'], r['60D'], r['R20'], r['R60']
        ])

    sync_to_google_sheet(TARGET_SHEET, matrix)

if __name__ == "__main__":
    run_super_growth_v13()
