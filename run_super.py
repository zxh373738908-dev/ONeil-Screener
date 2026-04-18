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
    print("📡 正在獲取全市場候選股...")
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
# 3. 核心量化模型 V10
# ==========================================
def run_super_growth_v10():
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    print("\n" + "="*50)
    print(f"🚀 [超級成長股 V10] 行業共振監控中...")
    
    # 1. 大盤與 VIX
    try:
        spy_df = yf.download("SPY", period="1y", interval="1d", progress=False)
        spy_close = spy_df['Close'].dropna()
        vix_val = float(yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1])
        curr_spy, ma50_spy = float(spy_close.iloc[-1]), float(spy_close.tail(50).mean())
        spy_ret = {5: get_return(spy_close, 5), 20: get_return(spy_close, 20), 60: get_return(spy_close, 60)}
        weather_icon = "☀️" if curr_spy > ma50_spy and vix_val < 20 else ("☁️" if curr_spy > ma50_spy or vix_val < 25 else "⛈️")
    except: weather_icon, vix_val, spy_ret = "❓", 0, {5:0, 20:0, 60:0}

    # 2. 下載技術面數據
    print(f"📡 下載全市場技術面數據...")
    hist_data = yf.download(UNIVERSE, period="1y", interval="1d", progress=False, threads=True)
    close_df, vol_df = hist_data['Close'], hist_data['Volume']
    high_df, low_df = hist_data['High'], hist_data['Low']

    tech_results = {}
    perfect_trend_tickers = [] # 記錄所有處於「完美多頭」的股票

    for t in UNIVERSE:
        try:
            if t not in close_df.columns: continue
            c, v, h, l = close_df[t].dropna(), vol_df[t].dropna(), high_df[t].dropna(), low_df[t].dropna()
            if len(c) < 150: continue
            
            p = float(c.iloc[-1])
            ma20, ma50, ma200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            
            is_perfect = p > ma20 > ma50 > ma200
            if is_perfect: perfect_trend_tickers.append(t)
            
            # 過濾進入基本面候選名單
            if not (p > ma50 and ma20 > ma50 and ma50 > ma200): continue
            if v.tail(40).mean() * p < 20_000_000: continue
            
            tech_results[t] = {
                "Price": p, "Trend": "完美多頭" if is_perfect else "多頭排列",
                "ADR": ((h - l) / l).tail(20).mean() * 100,
                "VolRatio": v.iloc[-1] / v.tail(20).mean() if v.tail(20).mean() > 0 else 1,
                "Bias": ((p - ma20) / ma20) * 100,
                "RS_Score": (get_return(c, 20) * 0.4) + (get_return(c, 60) * 0.3) + (get_return(c, 120) * 0.3),
                "5D%": get_return(c, 5), "20D%": get_return(c, 20), "60D%": get_return(c, 60),
                "REL 5": get_return(c, 5) - spy_ret[5], "REL 20": get_return(c, 20) - spy_ret[20], "REL 60": get_return(c, 60) - spy_ret[60]
            }
        except: continue

    # 3. 獲取基本面並計算「行業共振」
    print(f"✅ 全市場共振股: {len(perfect_trend_tickers)} 隻 | 獲取個股行業信息...")
    
    # 這裡我們需要獲取所有 perfect_trend_tickers 的行業，才能計算行業共振
    all_needed_tickers = list(set(list(tech_results.keys()) + perfect_trend_tickers))
    infos = {}
    with ThreadPoolExecutor(max_workers=15) as executor:
        for t, info in executor.map(fetch_info, all_needed_tickers):
            if info: infos[t] = info

    # 構建一個字典：{行業: [該行業中處於完美多頭的Ticker列表]}
    industry_resonance_map = {}
    for t in perfect_trend_tickers:
        if t in infos:
            ind = infos[t].get('industry', 'Unknown')
            if ind not in industry_resonance_map: industry_resonance_map[ind] = []
            industry_resonance_map[ind].append(t)

    # 4. 篩選與評分
    final_candidates = []
    rs_ranks = (pd.Series({t: d['RS_Score'] for t, d in tech_results.items()}).rank(pct=True) * 100).to_dict()
    
    for t, data in tech_results.items():
        if t not in infos: continue
        info = infos[t]
        industry = info.get('industry', 'Unknown')
        if any(ex.lower() in industry.lower() for ex in EXCLUDED_INDUSTRIES): continue
        rev_growth = info.get('revenueGrowth', 0)
        if rev_growth < -0.05: continue
        
        # 行業共振數：同一個行業裡，除了自己，還有多少隻股票也處於完美多頭
        group_resonance = len(industry_resonance_map.get(industry, []))
        
        data.update({
            "Ticker": t, "Industry": industry[:18], "RS Rank": rs_ranks.get(t, 0),
            "Score": (rs_ranks.get(t, 0) * 0.7) + (rev_growth * 100 * 0.3),
            "Resonance": group_resonance,
            "Market Cap": info.get('marketCap', 0) / 1_000_000
        })
        final_candidates.append(data)

    # 行業冠軍邏輯
    industry_winners = {}
    for s in final_candidates:
        ind = s['Industry']
        if ind not in industry_winners or s['Score'] > industry_winners[ind]['Score']:
            industry_winners[ind] = s
            
    top_final = sorted(industry_winners.values(), key=lambda x: x['Market Cap'])[:10]
    top_final.sort(key=lambda x: x['Score'], reverse=True)

    # ==========================================
    # 5. 輸出與對齊修復
    # ==========================================
    col_len = 16
    market_text = f"天气:{weather_icon} | 寬度:{(len(perfect_trend_tickers)/len(UNIVERSE)*100):.1f}% | 全市場共振:{len(perfect_trend_tickers)}隻 | VIX:{vix_val:.1f}"
    
    row1 = [f"SuperGrowth Portfolio V10", f"更新: {update_time}", market_text, "REL=相對SPY"] + [""] * (col_len - 4)
    # 調整列順序，將「行業共振」放在顯眼位置
    row2 = ["Ticker", "Industry", "評分", "趨勢狀態", "行業共振數", "ADR%(20d)", "量比(20d)", "乖離率(20d)", "MktCap(M)", "RS Rank", "Price", "5D%", "20D%", "60D%", "REL 20", "REL 60"]
    
    matrix = [row1, row2]
    for r in top_final:
        matrix.append([
            r['Ticker'], r['Industry'], round(r['Score'], 1), r['Trend'], 
            f"{r['Resonance']} 隻", # 行業共振數
            f"{round(r['ADR'], 2)}%", f"{round(r['VolRatio'], 2)}x", f"{round(r['Bias'], 2)}%",
            round(r['Market Cap'], 1), f"{round(r['RS Rank'], 1)}", round(r['Price'], 2),
            f"{round(r['5D%']*100, 2)}%", f"{round(r['20D%']*100, 2)}%", f"{round(r['60D%']*100, 2)}%",
            f"{round(r['REL 20']*100, 2)}%", f"{round(r['REL 60']*100, 2)}%"
        ])

    sync_to_google_sheet(TARGET_SHEET, matrix)
    print(f"✅ V10 行業共振同步成功！")

if __name__ == "__main__":
    run_super_growth_v10()
