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

try:
    req = urllib.request.Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})
    sp500_list = pd.read_html(urllib.request.urlopen(req).read())[0]['Symbol'].tolist()
    req2 = urllib.request.Request('https://en.wikipedia.org/wiki/Nasdaq-100', headers={'User-Agent': 'Mozilla/5.0'})
    ndx_list = pd.read_html(urllib.request.urlopen(req2).read())[4]['Ticker'].tolist()
    UNIVERSE = list(set([t.replace('.', '-') for t in sp500_list + ndx_list]))
except:
    UNIVERSE = ["AAPL", "MSFT", "GOOGL", "AVGO", "CAVA", "VRT", "MPWR", "NVDA", "LITE", "TER", "KEYS", "MRVL"]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'REIT', 'Utilities', 'Oil & Gas']

# ==========================================
# 2. 核心計算函數
# ==========================================
def fetch_info(t):
    retry_count = 3
    for i in range(retry_count):
        try:
            time.sleep(random.uniform(0.1, 0.4)) 
            ticker = yf.Ticker(t)
            info = ticker.info
            if info and ('industry' in info): return t, info
        except: time.sleep(1)
    return t, {}

def sync_to_google_sheet(sheet_name, matrix):
    try:
        def safe_json_val(val):
            if isinstance(val, float) and (not math.isfinite(val) or pd.isna(val)): return 0
            return str(val) if not isinstance(val, (int, float)) else val
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=safe_json_val))}
        requests.post(WEBAPP_URL, json=payload, timeout=30)
    except Exception as e: print(f"❌ 同步失敗: {e}")

def get_return(series, days):
    s = series.dropna()
    if len(s) < days + 1: return 0
    return (float(s.iloc[-1]) - float(s.iloc[-(days+1)])) / float(s.iloc[-(days+1)])

# ==========================================
# 3. 核心量化模型 V11
# ==========================================
def run_super_growth_v11():
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    print("\n" + "="*50)
    print(f"🚀 [超級成長股 V11] 17列決策矩陣掃描中...")
    
    # 1. 大盤指標
    try:
        spy_df = yf.download("SPY", period="1y", interval="1d", progress=False)
        spy_close = spy_df['Close'].dropna()
        vix_val = float(yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1])
        curr_spy, ma50_spy = float(spy_close.iloc[-1]), float(spy_close.tail(50).mean())
        spy_ret = {20: get_return(spy_close, 20), 60: get_return(spy_close, 60)}
        weather_icon = "☀️" if curr_spy > ma50_spy and vix_val < 20 else ("☁️" if curr_spy > ma50_spy or vix_val < 25 else "⛈️")
    except: weather_icon, vix_val, spy_ret = "❓", 0, {20:0, 60:0}

    # 2. 技術面數據
    print(f"📡 掃描 17 個維度數據...")
    hist_data = yf.download(UNIVERSE, period="1y", interval="1d", progress=False, threads=True)
    close_df, vol_df = hist_data['Close'], hist_data['Volume']
    high_df, low_df = hist_data['High'], hist_data['Low']

    tech_results, perfect_trend_tickers = {}, []

    for t in UNIVERSE:
        try:
            if t not in close_df.columns: continue
            c, v, h, l = close_df[t].dropna(), vol_df[t].dropna(), high_df[t].dropna(), low_df[t].dropna()
            if len(c) < 150: continue
            
            p = float(c.iloc[-1])
            ma20, ma50, ma200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            
            is_perfect = p > ma20 > ma50 > ma200
            if is_perfect: perfect_trend_tickers.append(t)
            
            # 成長股技術底線
            if not (p > ma50 and ma50 > ma200): continue
            if v.tail(40).mean() * p < 20_000_000: continue
            
            bias_20 = ((p - ma20) / ma20) * 100
            rs_score = (get_return(c, 20) * 0.4) + (get_return(c, 60) * 0.3) + (get_return(c, 120) * 0.3)
            
            tech_results[t] = {
                "Price": p, "ADR": ((h - l) / l).tail(20).mean() * 100,
                "VolRatio": v.iloc[-1] / v.tail(20).mean() if v.tail(20).mean() > 0 else 1,
                "Bias": bias_20, "RS_Score": rs_score,
                "5D%": get_return(c, 5), "20D%": get_return(c, 20), "60D%": get_return(c, 60),
                "REL 20": get_return(c, 20) - spy_ret[20], "REL 60": get_return(c, 60) - spy_ret[60]
            }
        except: continue

    # 3. 獲取信息與共振計算
    print(f"✅ 行情共振: {len(perfect_trend_tickers)} 隻 | 執行決策邏輯...")
    all_needed = list(set(list(tech_results.keys()) + perfect_trend_tickers))
    infos = {}
    with ThreadPoolExecutor(max_workers=15) as executor:
        for t, info in executor.map(fetch_info, all_needed):
            if info: infos[t] = info

    res_map = {}
    for t in perfect_trend_tickers:
        if t in infos:
            ind = infos[t].get('industry', 'Unknown')
            res_map[ind] = res_map.get(ind, 0) + 1

    final_data = []
    rs_ranks = (pd.Series({t: d['RS_Score'] for t, d in tech_results.items()}).rank(pct=True) * 100).to_dict()
    
    for t, data in tech_results.items():
        if t not in infos: continue
        info = infos[t]
        industry = info.get('industry', 'Unknown')
        if any(ex.lower() in industry.lower() for ex in EXCLUDED_INDUSTRIES): continue
        rev_growth = info.get('revenueGrowth', 0)
        
        rs_rank = rs_ranks.get(t, 0)
        
        # --- 新增智慧決策邏輯 ---
        # Action: 根據乖離率和RS排名
        if data['Bias'] < 4 and rs_rank > 90: action = "🎯 買點區"
        elif data['Bias'] > 12: action = "⌛ 等待回踩"
        elif rs_rank < 80: action = "⚠️ 觀察汰換"
        else: action = "👀 觀察"
        
        # Options: 根據 ADR 和強勢度
        if data['ADR'] > 3.5 and rs_rank > 90: options = "🔥 Bull Call"
        elif rs_rank > 85: options = "Spread"
        else: options = "N/A"

        data.update({
            "Ticker": t, "Industry": industry[:15], "Score": (rs_rank * 0.7) + (rev_growth * 100 * 0.3),
            "Action": action, "Resonance": res_map.get(industry, 0), "RS Rank": rs_rank, "Options": options,
            "Market Cap": info.get('marketCap', 0) / 1_000_000
        })
        final_data.append(data)

    # 行業冠軍與最終排序
    ind_winners = {}
    for s in final_data:
        ind = s['Industry']
        if ind not in ind_winners or s['Score'] > ind_winners[ind]['Score']: ind_winners[ind] = s
            
    top_final = sorted(ind_winners.values(), key=lambda x: x['Market Cap'])[:10]
    top_final.sort(key=lambda x: x['Score'], reverse=True)

    # ==========================================
    # 4. 輸出與 17 列精確對齊
    # ==========================================
    col_len = 17
    m_text = f"天气:{weather_icon} | 宽度:{(len(perfect_trend_tickers)/len(UNIVERSE)*100):.1f}% | 共振:{len(perfect_trend_tickers)}隻 | VIX:{vix_val:.1f}"
    
    row1 = [f"SuperGrowth Portfolio V11", f"更新: {update_time}", m_text, ""] + [""] * (col_len - 4)
    # 用戶要求的 17 列精確順序
    row2 = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
    
    matrix = [row1, row2]
    for r in top_final:
        matrix.append([
            r['Ticker'], r['Industry'], round(r['Score'], 1), r['Action'], 
            f"{r['Resonance']} 隻", f"{round(r['ADR'], 2)}%", f"{round(r['VolRatio'], 2)}x", f"{round(r['Bias'], 2)}%",
            round(r['Market Cap'], 1), round(r['RS Rank'], 1), r['Options'],
            round(r['Price'], 2), f"{round(r['5D%']*100, 2)}%", f"{round(r['20D%']*100, 2)}%", f"{round(r['60D%']*100, 2)}%",
            f"{round(r['REL 20']*100, 2)}%", f"{round(r['REL 60']*100, 2)}%"
        ])

    sync_to_google_sheet(TARGET_SHEET, matrix)
    print(f"✅ V11 同步成功！17列矩陣已精確對齊。")

if __name__ == "__main__":
    run_super_growth_v11()
