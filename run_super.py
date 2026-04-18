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

# 抓取全市場股票池
try:
    print("📡 正在獲取全市場 S&P 500 與 Nasdaq 100 股票池...")
    req = urllib.request.Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})
    sp500 = pd.read_html(urllib.request.urlopen(req).read())[0]['Symbol'].tolist()
    req2 = urllib.request.Request('https://en.wikipedia.org/wiki/Nasdaq-100', headers={'User-Agent': 'Mozilla/5.0'})
    ndx = pd.read_html(urllib.request.urlopen(req2).read())[4]['Ticker'].tolist()
    UNIVERSE = list(set([t.replace('.', '-') for t in sp500 + ndx]))
    print(f"✅ 成功獲取 {len(UNIVERSE)} 隻候選股！")
except Exception as e:
    print(f"⚠️ 獲取名單失敗，使用備用大型股池... ({e})")
    UNIVERSE = ["AAPL", "MSFT", "GOOGL", "AVGO", "CAVA", "FIVE", "MCK", "PWR", "VRT", "HWM", "NVDA", "META", "TSLA", "AMD", "LITE", "CIEN", "TER", "MPWR"]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'Credit', 'Real Estate', 'REIT', 'Utilities', 'Energy', 'Oil & Gas', 'Metals', 'Mining']

# ==========================================
# 2. 核心避錯與工具函數
# ==========================================
def fetch_info(t):
    """具備重試與隨機延遲的基本面獲取函數，避開 Yahoo 頻率限制"""
    retry_count = 3
    for i in range(retry_count):
        try:
            time.sleep(random.uniform(0.1, 0.4)) # 隨機微延遲
            ticker = yf.Ticker(t)
            info = ticker.info
            # 確保獲取到關鍵數據才算成功
            if info and 'totalRevenue' in info:
                return t, info
        except Exception as e:
            if i == retry_count - 1:
                return t, {}
            time.sleep(1.5) # 失敗後等待較長時間
    return t, {}

def sync_to_google_sheet(sheet_name, matrix):
    try:
        def safe_json_val(val):
            if isinstance(val, float) and (not math.isfinite(val) or pd.isna(val)): return 0
            return str(val) if not isinstance(val, (int, float)) else val
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=safe_json_val))}
        response = requests.post(WEBAPP_URL, json=payload, timeout=20)
        print(f"🎉 成功同步至 Google Sheets 分頁: [{sheet_name}]")
    except Exception as e: 
        print(f"❌ 同步失敗: {e}")

def get_return(series, days):
    s = series.dropna()
    if len(s) < days + 1: return 0
    return (float(s.iloc[-1]) - float(s.iloc[-(days+1)])) / float(s.iloc[-(days+1)])

# ==========================================
# 3. 核心量化模型 (一行業一冠軍版)
# ==========================================
def run_super_growth_v6():
    update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\n" + "="*50)
    print(f"🚀 [超級成長股 V6 - 行業冠軍版] 啟動 | 時間: {update_time}")
    
    # 大盤擇時
    try:
        spy_close = yf.download("SPY", period="1y", interval="1d", progress=False)['Close']
        if isinstance(spy_close, pd.DataFrame): spy_close = spy_close['SPY']
        curr_spy, ma50_spy = float(spy_close.iloc[-1]), float(spy_close.tail(50).mean())
        is_bull_market = curr_spy > ma50_spy
        spy_ret = {5: get_return(spy_close, 5), 20: get_return(spy_close, 20), 60: get_return(spy_close, 60)}
    except:
        is_bull_market, spy_ret = True, {5: 0, 20: 0, 60: 0} 
        
    MAX_STOCKS = 10 if is_bull_market else 3
    market_status_str = f"🟢多頭(滿倉10隻)" if is_bull_market else f"🔴弱勢(限縮3隻)"
    print(f"📈 大盤狀態: {market_status_str}")

    print(f"📡 批量下載股票技術特徵...")
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
            
            # 均線多頭 + VCP 波動收斂過濾
            if not (price > ma50 and ma20 > ma50 and ma50 > ma200): continue
            if v.tail(40).mean() * price < 20_000_000: continue
            if float(c.tail(15).std() / c.tail(15).mean()) > 0.15: continue
            
            ret_5, ret_20, ret_60, ret_120 = get_return(c, 5), get_return(c, 20), get_return(c, 60), get_return(c, 120)
            rs_scores[t] = (ret_20 * 0.4) + (ret_60 * 0.3) + (ret_120 * 0.3)
            
            valid_technical_pool[t] = {
                "Price": price, "5D%": ret_5, "20D%": ret_20, "60D%": ret_60,
                "REL 5": ret_5 - spy_ret[5], "REL 20": ret_20 - spy_ret[20], "REL 60": ret_60 - spy_ret[60]
            }
        except: continue

    if not valid_technical_pool: return
    rs_ranks = (pd.Series(rs_scores).rank(pct=True) * 100).to_dict()

    print(f"✅ 技術過濾剩餘 {len(valid_technical_pool)} 隻，並行獲取基本面 (含重試機制)...")
    infos = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        for t, info in executor.map(fetch_info, valid_technical_pool.keys()):
            infos[t] = info

    fundamental_candidates = []
    for t, info in infos.items():
        if not info or info.get('totalRevenue', 0) < 1_000_000_000: continue
        industry_str, sector_str = str(info.get('industry', '')), str(info.get('sector', ''))
        if any(ex.lower() in industry_str.lower() or ex.lower() in sector_str.lower() for ex in EXCLUDED_INDUSTRIES): continue
        
        rev_growth = info.get('revenueGrowth', 0) or 0
        if rev_growth <= 0: continue # 成長股硬性要求
        
        op_margin = info.get('operatingMargins', 0) or 0
        fcf_margin = (info.get('freeCashflow', 0) / info.get('totalRevenue', 1))
        
        # 綜合評分 (FG Score)：增長 + 利潤 + 現金流
        fg_score = (rev_growth * 100) + (op_margin * 100) + (fcf_margin * 100)
        
        data = valid_technical_pool[t]
        data.update({
            "Ticker": t, "Industry": industry_str[:18], 
            "Market Cap": info.get('marketCap', 0) / 1_000_000,
            "RS Rank": rs_ranks[t], "FG Score": fg_score
        })
        fundamental_candidates.append(data)

    # 🛡️ 核心邏輯：每個行業選出綜合評分 (FG Score) 最高的一隻
    industry_winners = {}
    for stock in fundamental_candidates:
        ind = stock['Industry']
        # 如果該行業還沒選過，或者當前股票的綜合分數比已選的高，就替換
        if ind not in industry_winners or stock['FG Score'] > industry_winners[ind]['FG Score']:
            industry_winners[ind] = stock
    
    # 從「行業冠軍」中選出符合動量要求 (RS > 75) 的池子
    champion_pool = [s for s in industry_winners.values() if s['RS Rank'] >= 75]
    
    # 最後在這些冠軍中，選取市值最小的 10 隻 (對齊 SuperGrowth 原意)
    top_final = sorted(champion_pool, key=lambda x: x['Market Cap'])[:MAX_STOCKS]
    top_final.sort(key=lambda x: x['RS Rank'], reverse=True)

    # ==========================================
    # 4. 輸出至 Google Sheets
    # ==========================================
    col_len = 11
    row1 = [f"SuperGrowth Portfolio", f"更新: {update_time}", f"大盤: {market_status_str}", "REL=相對SPY"] + [""] * (col_len - 4)
    row2 = ["Ticker", "Industry", "Price", "Market Cap(M)", "RS Rank", "5D%", "20D%", "60D%", "REL 5", "REL 20", "REL 60"]
    
    final_list = []
    for r in top_final:
        rs_str = f"⭐ {round(r['RS Rank'], 1)}" if r['RS Rank'] >= 90 else (f"⚠️ {round(r['RS Rank'], 1)}" if r['RS Rank'] < 80 else round(r['RS Rank'], 1))
        final_list.append([
            r['Ticker'], r['Industry'], round(r['Price'], 2), round(r['Market Cap'], 1),
            rs_str, f"{round(r['5D%']*100, 2)}%", f"{round(r['20D%']*100, 2)}%", f"{round(r['60D%']*100, 2)}%",
            f"{round(r['REL 5']*100, 2)}%", f"{round(r['REL 20']*100, 2)}%", f"{round(r['REL 60']*100, 2)}%"
        ])

    sync_to_google_sheet(TARGET_SHEET, [row1, row2] + final_list)
    print(f"\n📊 行業冠軍名單預覽 ({len(final_list)} 隻):")
    print(pd.DataFrame(final_list, columns=row2).to_string(index=False))

if __name__ == "__main__":
    run_super_growth_v6()
