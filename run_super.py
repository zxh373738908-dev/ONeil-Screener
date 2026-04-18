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
    retry_count = 2
    for i in range(retry_count):
        try:
            time.sleep(random.uniform(0.1, 0.3)) 
            ticker = yf.Ticker(t)
            info = ticker.info
            if info and ('marketCap' in info): return t, info
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
# 3. 核心量化模型 V9
# ==========================================
def run_super_growth_v9():
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    print("\n" + "="*50)
    print(f"🚀 [超級成長股 V9] 啟動 | 市場廣度與共振掃描中...")
    
    # 1. 大盤基礎與 VIX
    try:
        spy_df = yf.download("SPY", period="1y", interval="1d", progress=False)
        spy_close = spy_df['Close'].dropna()
        vix_val = float(yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1])
        curr_spy, ma50_spy = float(spy_close.iloc[-1]), float(spy_close.tail(50).mean())
        spy_ret = {5: get_return(spy_close, 5), 20: get_return(spy_close, 20), 60: get_return(spy_close, 60)}
        
        # ☀️ 天氣邏輯
        weather_icon = "☀️" if curr_spy > ma50_spy and vix_val < 20 else ("☁️" if curr_spy > ma50_spy or vix_val < 25 else "⛈️")
    except: weather_icon, vix_val, spy_ret = "❓", 0, {5:0, 20:0, 60:0}

    # 2. 全市場掃描 (技術面 + ADR% + 行情共振)
    print(f"📡 掃描 {len(UNIVERSE)} 隻個股行情共振狀態...")
    # 下載數據 (Close, High, Low, Volume)
    hist_data = yf.download(UNIVERSE, period="1y", interval="1d", progress=False, threads=True)
    close_df, high_df, low_df, vol_df = hist_data['Close'], hist_data['High'], hist_data['Low'], hist_data['Volume']

    resonance_count = 0  # 行情共振計數
    above_50ma_count = 0
    total_valid = 0
    valid_tech_pool = {}
    rs_scores = {}

    for t in UNIVERSE:
        try:
            if t not in close_df.columns: continue
            c, h, l, v = close_df[t].dropna(), high_df[t].dropna(), low_df[t].dropna(), vol_df[t].dropna()
            if len(c) < 150: continue
            
            curr_p = float(c.iloc[-1])
            ma20, ma50, ma200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            total_valid += 1
            if curr_p > ma50: above_50ma_count += 1
            
            # 定義完美趨勢 (行情共振基準): 價格 > 20MA > 50MA > 200MA
            is_perfect_trend = curr_p > ma20 > ma50 > ma200
            if is_perfect_trend: resonance_count += 1
            
            # 過濾入選股 (保持成長股基因)
            if not (curr_p > ma50 and ma20 > ma50 and ma50 > ma200): continue
            if v.tail(40).mean() * curr_p < 20_000_000: continue
            if float(c.tail(15).std() / c.tail(15).mean()) > 0.15: continue
            
            # --- 計算 V9 新指標 ---
            # ADR% (20d): 最近20天的平均每日波動幅度 (High-Low)/Low
            adr_20 = ((h - l) / l).tail(20).mean() * 100
            # 量比 (20d): 今日成交量 / 20日平均成交量
            vol_ratio = v.iloc[-1] / v.tail(20).mean() if v.tail(20).mean() > 0 else 1
            # 乖離率 (20d): 價格距離 20MA 的百分比
            bias_20 = ((curr_p - ma20) / ma20) * 100
            
            rs_scores[t] = (get_return(c, 20) * 0.4) + (get_return(c, 60) * 0.3) + (get_return(c, 120) * 0.3)
            
            valid_tech_pool[t] = {
                "Price": curr_p, "Trend": "完美多頭" if is_perfect_trend else "多頭排列",
                "ADR": adr_20, "VolRatio": vol_ratio, "Bias": bias_20,
                "5D%": get_return(c, 5), "20D%": get_return(c, 20), "60D%": get_return(c, 60),
                "REL 5": get_return(c, 5) - spy_ret[5], "REL 20": get_return(c, 20) - spy_ret[20], "REL 60": get_return(c, 60) - spy_ret[60]
            }
        except: continue

    # 3. 獲取基本面與最終評分
    market_breadth = (above_50ma_count / total_valid * 100) if total_valid > 0 else 0
    print(f"✅ 寬度: {market_breadth:.1f}% | 共振: {resonance_count} 隻 | 獲取基本面...")
    
    infos = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        for t, info in executor.map(fetch_info, valid_tech_pool.keys()): infos[t] = info

    final_candidates = []
    rs_ranks = (pd.Series(rs_scores).rank(pct=True) * 100).to_dict()
    
    for t, info in infos.items():
        if not info: continue
        industry = str(info.get('industry', 'Unknown'))
        if any(ex.lower() in industry.lower() for ex in EXCLUDED_INDUSTRIES): continue
        rev_growth = info.get('revenueGrowth', 0)
        if rev_growth < -0.05: continue 
        
        # 評分系統 (RS Rank 權重 70% + 基本面增長 30%)
        total_score = (rs_ranks[t] * 0.7) + (rev_growth * 100 * 0.3)
        
        data = valid_tech_pool[t]
        data.update({
            "Ticker": t, "Industry": industry[:15], "Score": total_score, 
            "Market Cap": info.get('marketCap', 0) / 1_000_000, "RS Rank": rs_ranks[t]
        })
        fundamental_candidates.append(data)

    # 行業冠軍選取 (1行業1隻)
    industry_winners = {}
    for s in fundamental_candidates:
        ind = s['Industry']
        if ind not in industry_winners or s['Score'] > industry_winners[ind]['Score']:
            industry_winners[ind] = s
            
    top_final = sorted(industry_winners.values(), key=lambda x: x['Market Cap'])[:10]
    top_final.sort(key=lambda x: x['Score'], reverse=True)

    # ==========================================
    # 4. 輸出至 Google Sheets
    # ==========================================
    col_len = 16 # 擴展欄位數
    weather_text = f"当前天气: {weather_icon} | 宽度(50MA): {market_breadth:.1f}% | 共振: {resonance_count}隻 | VIX指数: {vix_val:.1f}"
    row1 = [f"SuperGrowth Portfolio", f"更新: {update_time}", weather_text, "REL=相對SPY"] + [""] * (col_len - 4)
    row2 = ["Ticker", "Industry", "评分", "趋势状态", "ADR%(20d)", "量比(20d)", "乖离率(20d)", "MktCap(M)", "RS Rank", "Price", "5D%", "20D%", "60D%", "REL 5", "REL 20", "REL 60"]
    
    final_list = []
    for r in top_final:
        final_list.append([
            r['Ticker'], r['Industry'], round(r['Score'], 1), r['Trend'], 
            f"{round(r['ADR'], 2)}%", f"{round(r['VolRatio'], 2)}x", f"{round(r['Bias'], 2)}%",
            round(r['Market Cap'], 1), f"{round(r['RS Rank'], 1)}", round(r['Price'], 2),
            f"{round(r['5D%']*100, 2)}%", f"{round(r['20D%']*100, 2)}%", f"{round(r['60D%']*100, 2)}%",
            f"{round(r['REL 5']*100, 2)}%", f"{round(r['REL 20']*100, 2)}%", f"{round(r['REL 60']*100, 2)}%"
        ])

    sync_to_google_sheet(TARGET_SHEET, [row1, row2] + final_list)
    print(f"✅ V9 同步完畢 | 市場共振強度: {resonance_count}")

if __name__ == "__main__":
    run_super_growth_v9()
