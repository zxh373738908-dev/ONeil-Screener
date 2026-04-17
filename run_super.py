import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import requests
import json
import warnings
import math
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

# ==========================================
# 1. 系統配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycby1pIM7iO43lcLQpOmi5LCJIn3VN9a0Ilf9amoy1EtQV_GBXJkk_A4PpsrJxKzH7i51/exec"
TARGET_SHEET = "super"

# 建立全局 Session 偽裝，解決 Invalid Crumb 報錯
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

# 偽裝請求抓取維基百科標普500與納指100，解決 403 Forbidden
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
    UNIVERSE =["AAPL", "MSFT", "GOOGL", "AVGO", "CAVA", "FIVE", "MCK", "PWR", "VRT", "HWM", "NVDA", "META", "TSLA", "AMD"]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'Credit Services']

def sync_to_google_sheet(sheet_name, matrix):
    try:
        def safe_json_val(val):
            if isinstance(val, float) and (not math.isfinite(val) or pd.isna(val)): return 0
            return str(val) if not isinstance(val, (int, float)) else val
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=safe_json_val))}
        response = requests.post(WEBAPP_URL, json=payload, timeout=20)
        print(f"🎉 成功同步 {len(matrix)-2} 隻股票至 Google Sheets -> 分頁: [{sheet_name}]")
    except Exception as e: 
        print(f"❌ 同步失敗: {e}")

def get_return(series, days):
    s = series.dropna()
    if len(s) < days + 1: return 0
    return (float(s.iloc[-1]) - float(s.iloc[-(days+1)])) / float(s.iloc[-(days+1)])

# ==========================================
# 2. 核心量化模型 (漏斗過濾機制)
# ==========================================
def run_super_growth_funnel():
    print("\n" + "="*50)
    print("🚀[超級成長股 - 漏斗過濾版] 啟動掃描...")
    
    # 獲取大盤基準
    try:
        spy_close = yf.download("SPY", period="1y", interval="1d", session=session, progress=False)['Close']
        if isinstance(spy_close, pd.DataFrame): spy_close = spy_close['SPY']
        spy_ret = {5: get_return(spy_close, 5), 20: get_return(spy_close, 20), 60: get_return(spy_close, 60)}
    except:
        spy_ret = {5: 0, 20: 0, 60: 0} # 容錯處理

    print(f"📡 批量下載股票技術特徵 (這可能需要幾十秒)...")
    hist_data = yf.download(UNIVERSE, period="1y", interval="1d", session=session, progress=False, threads=True)
    
    close_df = hist_data['Close']
    vol_df = hist_data['Volume']

    valid_technical_pool = {}
    rs_scores = {}

    # 【漏斗 1】：技術面與動量一票否決
    for t in UNIVERSE:
        try:
            if t not in close_df.columns: continue
            c = close_df[t].dropna()
            v = vol_df[t].dropna()
            if len(c) < 150: continue
            
            price = float(c.iloc[-1])
            ma20, ma50, ma200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            
            if not (ma20 > ma50 and ma50 > ma200): continue
            if v.tail(40).mean() * price < 20_000_000: continue
            
            ret_5, ret_20, ret_60, ret_120 = get_return(c, 5), get_return(c, 20), get_return(c, 60), get_return(c, 120)
            
            rs_scores[t] = (ret_20 * 0.4) + (ret_60 * 0.3) + (ret_120 * 0.3)
            
            valid_technical_pool[t] = {
                "Price": price, "5D%": ret_5, "20D%": ret_20, "60D%": ret_60,
                "REL 5": ret_5 - spy_ret[5], "REL 20": ret_20 - spy_ret[20], "REL 60": ret_60 - spy_ret[60]
            }
        except: continue

    if not valid_technical_pool:
        print("❌ 市場極度惡劣，無任何股票滿足技術面條件！")
        return

    rs_series = pd.Series(rs_scores)
    rs_ranks = (rs_series.rank(pct=True) * 100).to_dict()

    print(f"✅ 技術過濾剩餘 {len(valid_technical_pool)} 隻，開始並行獲取基本面...")
    
    def fetch_info(t):
        try: 
            ticker = yf.Ticker(t)
            ticker.session = session
            return t, ticker.info
        except: return t, {}
        
    infos = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        for t, info in executor.map(fetch_info, valid_technical_pool.keys()):
            infos[t] = info

    fundamental_candidates =[]
    for t, info in infos.items():
        if not info: continue
        
        if info.get('totalRevenue', 0) < 1_000_000_000: continue
        if any(ex.lower() in str(info.get('sector', '')).lower() for ex in EXCLUDED_INDUSTRIES): continue
        if info.get('marketCap', 0) == 0: continue
        
        rev_growth = info.get('revenueGrowth', 0) or 0
        op_margin = info.get('operatingMargins', 0) or 0
        fcf = info.get('freeCashflow', 0) or 0
        rev = info.get('totalRevenue', 1)
        fcf_margin = fcf / rev if rev > 0 else 0
        
        # 放寬了強勢股的淨利潤要求，避免錯殺高增長公司
        if op_margin <= -0.1: continue 
            
        fg_score = (rev_growth * 100) + (op_margin * 100) + (fcf_margin * 100)
        
        data = valid_technical_pool[t]
        data.update({
            "Ticker": t, "Industry": str(info.get('industry', ''))[:15], 
            "Market Cap": info.get('marketCap') / 1_000_000,
            "RS Rank": rs_ranks[t], "FG Score": fg_score
        })
        fundamental_candidates.append(data)

    fundamental_candidates.sort(key=lambda x: x['FG Score'], reverse=True)
    top_tier_pool = fundamental_candidates[:30]

    # 從 Top 30 裡挑選市值最小的 10 隻
    top_10 = sorted(top_tier_pool, key=lambda x: x['Market Cap'])[:10]
    top_10.sort(key=lambda x: x['RS Rank'], reverse=True)

    # ==========================================
    # 3. 輸出至 Google Sheets (嚴格對齊矩陣)
    # ==========================================
    # ⚠️ 這裡非常關鍵：確保上下行的陣列長度嚴格等於 11，防止 Google Sheets 報錯！
    col_len = 11
    row1 =["SuperGrowth Portfolio", f"更新: {datetime.datetime.now().strftime('%Y-%m-%d')}", "REL=相對SPY報酬"] + [""] * (col_len - 3)
    row2 =["Ticker", "Industry", "Price", "Market Cap(M)", "RS Rank", "5D%", "20D%", "60D%", "REL 5", "REL 20", "REL 60"]
    
    header =[row1, row2]
    
    final_list =[]
    for r in top_10:
        rs_str = f"⭐ {round(r['RS Rank'], 1)}" if r['RS Rank'] >= 90 else (f"⚠️ {round(r['RS Rank'], 1)}" if r['RS Rank'] < 80 else round(r['RS Rank'], 1))
        
        final_list.append([
            r['Ticker'], r['Industry'], round(r['Price'], 2), round(r['Market Cap'], 1),
            rs_str, 
            f"{round(r['5D%']*100, 2)}%", f"{round(r['20D%']*100, 2)}%", f"{round(r['60D%']*100, 2)}%",
            f"{round(r['REL 5']*100, 2)}%", f"{round(r['REL 20']*100, 2)}%", f"{round(r['REL 60']*100, 2)}%"
        ])

    matrix_to_upload = header + final_list
    sync_to_google_sheet(TARGET_SHEET, matrix_to_upload)
    
    print("\n📊 入選 10 隻超級成長股預覽:")
    df_show = pd.DataFrame(final_list, columns=row2)
    print(df_show.to_string(index=False))

if __name__ == "__main__":
    run_super_growth_funnel()
