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

# 獲取全市場股票池 (SP500 + Nasdaq 100)
try:
    print("📡 正在獲取全市場候選股...")
    req = urllib.request.Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})
    sp500 = pd.read_html(urllib.request.urlopen(req).read())[0]['Symbol'].tolist()
    req2 = urllib.request.Request('https://en.wikipedia.org/wiki/Nasdaq-100', headers={'User-Agent': 'Mozilla/5.0'})
    ndx = pd.read_html(urllib.request.urlopen(req2).read())[4]['Ticker'].tolist()
    UNIVERSE = list(set([t.replace('.', '-') for t in sp500 + ndx]))
    print(f"✅ 成功獲取 {len(UNIVERSE)} 隻候選股！")
except:
    UNIVERSE = ["AAPL", "MSFT", "GOOGL", "AVGO", "CAVA", "VRT", "MPWR", "NVDA", "LITE", "TER", "KEYS", "MRVL"]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'REIT', 'Utilities', 'Oil & Gas']

# ==========================================
# 2. 核心避錯與工具函數
# ==========================================
def fetch_info(t):
    """具備重試與隨機延遲，解決 401 Crumb 報錯"""
    retry_count = 3
    for i in range(retry_count):
        try:
            time.sleep(random.uniform(0.1, 0.4)) 
            ticker = yf.Ticker(t)
            info = ticker.info
            if info and ('marketCap' in info): # 只要有市值就視為有效抓取
                return t, info
        except:
            if i == retry_count - 1: return t, {}
            time.sleep(1)
    return t, {}

def sync_to_google_sheet(sheet_name, matrix):
    try:
        def safe_json_val(val):
            if isinstance(val, float) and (not math.isfinite(val) or pd.isna(val)): return 0
            return str(val) if not isinstance(val, (int, float)) else val
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=safe_json_val))}
        requests.post(WEBAPP_URL, json=payload, timeout=25)
        print(f"🎉 同步成功至分頁: [{sheet_name}]")
    except Exception as e: print(f"❌ 同步失敗: {e}")

def get_return(series, days):
    s = series.dropna()
    if len(s) < days + 1: return 0
    return (float(s.iloc[-1]) - float(s.iloc[-(days+1)])) / float(s.iloc[-(days+1)])

# ==========================================
# 3. 核心量化模型 V7 (智能補位版)
# ==========================================
def run_super_growth_v7():
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\n" + "="*50)
    print(f"🚀 [超級成長股 V7] 啟動 | 更新時間: {update_time}")
    
    # 1. 大盤擇時
    try:
        spy_close = yf.download("SPY", period="1y", interval="1d", progress=False)['Close']
        if isinstance(spy_close, pd.DataFrame): spy_close = spy_close['SPY']
        curr_spy, ma50_spy = float(spy_close.iloc[-1]), float(spy_close.tail(50).mean())
        is_bull_market = curr_spy > ma50_spy
        spy_ret = {5: get_return(spy_close, 5), 20: get_return(spy_close, 20), 60: get_return(spy_close, 60)}
    except: is_bull_market, spy_ret = True, {5: 0, 20: 0, 60: 0} 
        
    market_status_str = f"🟢多頭(滿倉)" if is_bull_market else f"🔴弱勢(防禦)"
    MAX_TARGET = 10 if is_bull_market else 3

    # 2. 技術面過濾
    print(f"📡 批量下載技術特徵...")
    hist_data = yf.download(UNIVERSE, period="1y", interval="1d", progress=False, threads=True)
    close_df, vol_df = hist_data['Close'], hist_data['Volume']

    valid_technical_pool, rs_scores = {}, {}

    for t in UNIVERSE:
        try:
            if t not in close_df.columns: continue
            c, v = close_df[t].dropna(), vol_df[t].dropna()
            if len(c) < 150: continue
            
            price = float(c.iloc[-1])
            ma20, ma50, ma200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            
            # 多頭過濾 + VCP (0.15)
            if not (price > ma50 and ma20 > ma50 and ma50 > ma200): continue
            if v.tail(40).mean() * price < 20_000_000: continue
            if float(c.tail(15).std() / c.tail(15).mean()) > 0.15: continue
            
            # 計算 RVOL (相對成交量：最近 5 天 vs 60 天平均)
            rvol = v.tail(5).mean() / v.tail(60).mean() if v.tail(60).mean() > 0 else 1
            
            ret_5, ret_20, ret_60, ret_120 = get_return(c, 5), get_return(c, 20), get_return(c, 60), get_return(c, 120)
            rs_scores[t] = (ret_20 * 0.4) + (ret_60 * 0.3) + (ret_120 * 0.3)
            
            valid_technical_pool[t] = {
                "Price": price, "RVOL": rvol, "5D%": ret_5, "20D%": ret_20, "60D%": ret_60,
                "REL 5": ret_5 - spy_ret[5], "REL 20": ret_20 - spy_ret[20], "REL 60": ret_60 - spy_ret[60]
            }
        except: continue

    if not valid_technical_pool: return
    rs_ranks = (pd.Series(rs_scores).rank(pct=True) * 100).to_dict()

    # 3. 並行獲取基本面
    print(f"✅ 技術過濾剩餘 {len(valid_technical_pool)} 隻，開始獲取基本面...")
    infos = {}
    with ThreadPoolExecutor(max_workers=12) as executor:
        for t, info in executor.map(fetch_info, valid_technical_pool.keys()):
            infos[t] = info

    fundamental_candidates = []
    for t, info in infos.items():
        if not info: continue
        industry_str = str(info.get('industry', 'Unknown'))
        if any(ex.lower() in industry_str.lower() for ex in EXCLUDED_INDUSTRIES): continue
        
        # 數據容錯：嘗試多個成長字段防止誤殺
        rev_growth = info.get('revenueGrowth') or info.get('earningsQuarterlyGrowth') or 0
        if rev_growth < -0.05: continue 
        
        op_margin = info.get('operatingMargins', 0) or 0
        fcf_margin = (info.get('freeCashflow', 0) / info.get('totalRevenue', 1)) if info.get('totalRevenue', 0) > 0 else 0
        
        # 綜合評分：營收成長 + 利潤率 + 現金流
        fg_score = (rev_growth * 100) + (op_margin * 100) + (fcf_margin * 100)
        
        data = valid_technical_pool[t]
        data.update({
            "Ticker": t, "Industry": industry_str[:18], 
            "Market Cap": info.get('marketCap', 0) / 1_000_000,
            "RS Rank": rs_ranks[t], "FG Score": fg_score
        })
        fundamental_candidates.append(data)

    # 🛡️ 4. 核心：階梯式補位邏輯
    # 先按行業分組，每組內按分數排序
    ind_groups = {}
    for s in fundamental_candidates:
        ind = s['Industry']
        if ind not in ind_groups: ind_groups[ind] = []
        ind_groups[ind].append(s)
    
    for ind in ind_groups:
        ind_groups[ind] = sorted(ind_groups[ind], key=lambda x: x['FG Score'], reverse=True)

    champion_pool = []
    # 第一輪：挑選每個行業的「第一名」(RS 門檻 75)
    for ind in ind_groups:
        if ind_groups[ind][0]['RS Rank'] >= 75:
            champion_pool.append(ind_groups[ind][0])
            
    # 第二輪：如果選不滿 10 隻，從強勢行業挑選「第二名」(RS 門檻提高到 80，確保品質)
    if len(champion_pool) < MAX_TARGET:
        for ind in ind_groups:
            if len(ind_groups[ind]) > 1:
                runner_up = ind_groups[ind][1]
                if runner_up['RS Rank'] >= 80:
                    champion_pool.append(runner_up)
            if len(champion_pool) >= MAX_TARGET: break

    # 最終選取：在這些精英中，挑選市值最小的（對齊超級成長股原意）
    top_final = sorted(champion_pool, key=lambda x: x['Market Cap'])[:MAX_TARGET]
    top_final.sort(key=lambda x: x['RS Rank'], reverse=True)

    # ==========================================
    # 5. 輸出至 Google Sheets
    # ==========================================
    col_len = 12
    row1 = [f"SuperGrowth Portfolio", f"更新: {update_time}", f"大盤: {market_status_str}", "REL=相對SPY"] + [""] * (col_len - 4)
    row2 = ["Ticker (Link)", "Industry", "Price", "Mkt Cap(M)", "RS Rank", "RVOL", "5D%", "20D%", "60D%", "REL 5", "REL 20", "REL 60"]
    
    final_list = []
    for r in top_final:
        rs_str = f"⭐ {round(r['RS Rank'], 1)}" if r['RS Rank'] >= 90 else (f"⚠️ {round(r['RS Rank'], 1)}" if r['RS Rank'] < 80 else round(r['RS Rank'], 1))
        # 💡 生成 Yahoo Finance 點擊連結
        ticker_link = f'=HYPERLINK("https://finance.yahoo.com/quote/{r["Ticker"]}","{r["Ticker"]}")'
        
        final_list.append([
            ticker_link, r['Industry'], round(r['Price'], 2), round(r['Market Cap'], 1),
            rs_str, f"{round(r['RVOL'], 2)}x", f"{round(r['5D%']*100, 2)}%", f"{round(r['20D%']*100, 2)}%", f"{round(r['60D%']*100, 2)}%",
            f"{round(r['REL 5']*100, 2)}%", f"{round(r['REL 20']*100, 2)}%", f"{round(r['REL 60']*100, 2)}%"
        ])

    sync_to_google_sheet(TARGET_SHEET, [row1, row2] + final_list)
    print(f"\n✅ 篩選完成！共入選 {len(final_list)} 隻股票。")

if __name__ == "__main__":
    run_super_growth_v7()
