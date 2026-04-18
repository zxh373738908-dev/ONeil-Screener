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
        return ["AAPL", "MSFT", "GOOGL", "AVGO", "CAVA", "VRT", "MPWR", "NVDA", "LITE", "TER", "KEYS", "MRVL", "FIX"]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'REIT', 'Utilities', 'Oil & Gas']

# ==========================================
# 2. 核心避錯與數據函數
# ==========================================
def fetch_info_safe(t):
    """基本面獲取：增加延遲與錯誤重試，失敗則返回空字典"""
    for i in range(2):
        try:
            time.sleep(random.uniform(0.5, 1.0))
            ticker = yf.Ticker(t)
            info = ticker.info
            if info and 'industry' in info:
                return t, info
        except:
            time.sleep(1)
    return t, {}

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
# 3. 核心量化模型 V12 (17列精準版)
# ==========================================
def run_super_growth_v12():
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    universe_tickers = get_universe()
    
    print("\n" + "="*50)
    print(f"🚀 [超級成長股 V12] 啟動 | 正在執行全環境適配掃描...")
    
    # 1. 大盤與 VIX 指標
    try:
        spy_hist = yf.download("SPY", period="1y", interval="1d", progress=False)['Close'].dropna()
        vix_val = float(yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1])
        curr_spy, ma50_spy = float(spy_hist.iloc[-1]), float(spy_hist.tail(50).mean())
        spy_ret = {20: get_return(spy_hist, 20), 60: get_return(spy_hist, 60)}
        weather_icon = "☀️" if curr_spy > ma50_spy and vix_val < 20 else ("☁️" if curr_spy > ma50_spy or vix_val < 25 else "⛈️")
    except:
        weather_icon, vix_val, spy_ret = "❓", 0, {20:0, 60:0}

    # 2. 下載技術面 (批量)
    hist = yf.download(universe_tickers, period="1y", interval="1d", progress=False, threads=True)
    close_df, vol_df, high_df, low_df = hist['Close'], hist['Volume'], hist['High'], hist['Low']

    tech_data, perfect_tickers = {}, []
    for t in universe_tickers:
        try:
            if t not in close_df.columns: continue
            c, v, h, l = close_df[t].dropna(), vol_df[t].dropna(), high_df[t].dropna(), low_df[t].dropna()
            if len(c) < 150: continue
            
            p = float(c.iloc[-1])
            m20, m50, m200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            
            is_perfect = p > m20 > m50 > m200
            if is_perfect: perfect_tickers.append(t)
            
            # 過濾門檻：需在 50MA 上方，且流動性達標
            if not (p > m50 and m50 > m200): continue
            if v.tail(40).mean() * p < 20_000_000: continue
            
            # VCP 波動率過濾 (防止暴漲暴跌)
            if float(c.tail(15).std() / c.tail(15).mean()) > 0.15: continue
            
            rs_score = (get_return(c, 20) * 0.4) + (get_return(c, 60) * 0.3) + (get_return(c, 120) * 0.3)
            tech_data[t] = {
                "Price": p, "ADR": ((h - l) / l).tail(20).mean() * 100,
                "VolRatio": v.iloc[-1] / v.tail(20).mean() if v.tail(20).mean() > 0 else 1,
                "Bias": ((p - m20) / m20) * 100, "RS_Raw": rs_score,
                "5D": get_return(c, 5), "20D": get_return(c, 20), "60D": get_return(c, 60),
                "R20": get_return(c, 20) - spy_ret[20], "R60": get_return(c, 60) - spy_ret[60],
                "Trend": "完美多頭" if is_perfect else "上升通道"
            }
        except: continue

    # 3. 獲取基本面 (慢速安全模式)
    print(f"✅ 行情共振股: {len(perfect_tickers)} 隻 | 正在獲取基本面 (安全模式)...")
    infos = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        for t, info in executor.map(fetch_info_safe, list(tech_data.keys())):
            if info: infos[t] = info

    # 行業共振映射
    res_map = {}
    for t in perfect_tickers:
        ind = infos.get(t, {}).get('industry', 'Unknown')
        res_map[ind] = res_map.get(ind, 0) + 1

    # 4. 決策矩陣與評分
    final_list = []
    rs_ranks = (pd.Series({t: d['RS_Raw'] for t, d in tech_data.items()}).rank(pct=True) * 100).to_dict()
    
    for t, data in tech_data.items():
        # 如果 info 抓取失敗 (401)，我們依然保留該股，只是行業顯示 Missing
        info = infos.get(t, {})
        industry = info.get('industry', 'Data Missing')
        if any(ex.lower() in industry.lower() for ex in EXCLUDED_INDUSTRIES): continue
        
        rs = rs_ranks.get(t, 0)
        # 評分：動能佔 70%，基本面增長佔 30% (數據缺失則基本面部分為0)
        score = (rs * 0.7) + ((info.get('revenueGrowth', 0) or 0) * 100 * 0.3)
        
        # 智慧 Action 邏輯
        if data['Bias'] < 4 and rs > 90: action = "🎯 買點區"
        elif data['Bias'] > 12: action = "⌛ 等待回踩"
        elif rs < 80: action = "⚠️ 觀察汰換"
        else: action = "👀 觀察"
        
        # 期權建議
        opt = "🔥 Call" if data['ADR'] > 3.5 and rs > 90 else "N/A"

        final_list.append({
            "Ticker": t, "Industry": industry[:18], "Score": score, "Action": action,
            "Resonance": f"{res_map.get(industry, 0)} 隻", "ADR": data['ADR'],
            "Vol": data['VolRatio'], "Bias": data['Bias'], "MktCap": info.get('marketCap', 0) / 1_000_000,
            "RS": rs, "Opt": opt, "Price": data['Price'], "5D": data['5D'], "20D": data['20D'],
            "60D": data['60D'], "R20": data['R20'], "R60": data['R60']
        })

    # 行業冠軍選取與最終排序
    top_10 = sorted(final_list, key=lambda x: x['Score'], reverse=True)[:10]

    # ==========================================
    # 5. 輸出至 Google Sheets (精確對齊 17 列)
    # ==========================================
    m_text = f"天气:{weather_icon} | 宽度:{(len(perfect_tickers)/len(universe_tickers)*100):.1f}% | 共振:{len(perfect_tickers)}隻 | VIX:{vix_val:.1f}"
    row1 = [f"SuperGrowth Portfolio V12", f"更新: {update_time}", m_text, ""] + [""] * 13
    row2 = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap(M)", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
    
    matrix = [row1, row2]
    for r in top_10:
        matrix.append([
            r['Ticker'], r['Industry'], round(r['Score'], 1), r['Action'], r['Resonance'],
            f"{round(r['ADR'], 2)}%", f"{round(r['Vol'], 2)}x", f"{round(r['Bias'], 2)}%",
            round(r['MktCap'], 1), round(r['RS'], 1), r['Opt'],
            float(r['Price']), # Price 是數字，L列請在Sheets設為「數值」
            f"{round(r['5D']*100, 2)}%", f"{round(r['20D']*100, 2)}%", f"{round(r['60D']*100, 2)}%",
            f"{round(r['R20']*100, 2)}%", f"{round(r['R60']*100, 2)}%"
        ])

    sync_to_google_sheet(TARGET_SHEET, matrix)
    print(f"✅ V12 同步完畢。已對 401 錯誤標的執行數據補償。")

if __name__ == "__main__":
    run_super_growth_v12()
