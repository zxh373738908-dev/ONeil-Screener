import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import requests
import json
import warnings
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

# ==========================================
# 1. 系統配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycby1pIM7iO43lcLQpOmi5LCJIn3VN9a0Ilf9amoy1EtQV_GBXJkk_A4PpsrJxKzH7i51/exec"
TARGET_SHEET = "super"

# 選股池 (建議實戰使用羅素1000)
try:
    sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]['Symbol'].tolist()
    ndx = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[4]['Ticker'].tolist()
    UNIVERSE = list(set([t.replace('.', '-') for t in sp500 + ndx]))
except:
    UNIVERSE =["AAPL", "MSFT", "GOOGL", "AVGO", "CAVA", "FIVE", "MCK", "PWR", "VRT", "HWM"]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'Credit Services']

def sync_to_google_sheet(sheet_name, matrix):
    try:
        def safe_json_val(val):
            if isinstance(val, float) and (not math.isfinite(val) or pd.isna(val)): return 0
            return str(val) if not isinstance(val, (int, float)) else val
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=safe_json_val))}
        requests.post(WEBAPP_URL, json=payload, timeout=20)
        print(f"🎉 成功同步數據至 Google Sheets -> 分頁: [{sheet_name}]")
    except Exception as e: print(f"❌ 同步失敗: {e}")

def get_return(series, days):
    s = series.dropna()
    if len(s) < days + 1: return 0
    return (float(s.iloc[-1]) - float(s.iloc[-(days+1)])) / float(s.iloc[-(days+1)])

# ==========================================
# 2. 核心量化模型 (漏斗過濾機制)
# ==========================================
def run_super_growth_funnel():
    print("\n" + "="*50)
    print("🚀 [超級成長股 - 漏斗過濾版] 啟動掃描...")
    
    # 獲取大盤基準
    spy_close = yf.download("SPY", period="1y", interval="1d", progress=False)['Close']
    if isinstance(spy_close, pd.DataFrame): spy_close = spy_close['SPY']
    spy_ret = {5: get_return(spy_close, 5), 20: get_return(spy_close, 20), 60: get_return(spy_close, 60)}

    print(f"📡 批量下載 {len(UNIVERSE)} 隻股票技術特徵...")
    hist_data = yf.download(UNIVERSE, period="1y", interval="1d", progress=False)
    close_df = hist_data['Close']
    vol_df = hist_data['Volume']

    valid_technical_pool = {}
    rs_scores = {}

    # 【漏斗 1】：技術面與動量一票否決 (規則 5)
    for t in UNIVERSE:
        try:
            c = close_df[t].dropna()
            v = vol_df[t].dropna()
            if len(c) < 150: continue
            
            price = float(c.iloc[-1])
            ma20, ma50, ma200 = c.tail(20).mean(), c.tail(50).mean(), c.tail(200).mean()
            
            # 趨勢與流動性過濾
            if not (ma20 > ma50 and ma50 > ma200): continue
            if v.tail(40).mean() * price < 20_000_000: continue
            
            ret_5, ret_20, ret_60, ret_120 = get_return(c, 5), get_return(c, 20), get_return(c, 60), get_return(c, 120)
            
            # 綜合動量分 (RS Rank 的基礎)
            rs_scores[t] = (ret_20 * 0.4) + (ret_60 * 0.3) + (ret_120 * 0.3)
            
            valid_technical_pool[t] = {
                "Price": price, "5D%": ret_5, "20D%": ret_20, "60D%": ret_60,
                "REL 5": ret_5 - spy_ret[5], "REL 20": ret_20 - spy_ret[20], "REL 60": ret_60 - spy_ret[60]
            }
        except: continue

    if not valid_technical_pool:
        print("❌ 市場極度惡劣，無任何股票滿足技術面條件！")
        return

    # 計算全市場 RS Rank
    rs_series = pd.Series(rs_scores)
    rs_ranks = (rs_series.rank(pct=True) * 100).to_dict()

    print(f"✅ 技術過濾剩餘 {len(valid_technical_pool)} 隻，開始並行獲取基本面...")
    
    def fetch_info(t):
        try: return t, yf.Ticker(t).info
        except: return t, {}
        
    infos = {}
    with ThreadPoolExecutor(max_workers=15) as executor:
        for t, info in executor.map(fetch_info, valid_technical_pool.keys()):
            infos[t] = info

    # 【漏斗 2】：基本面強度打分 (規則 1, 2, 3, 4)
    fundamental_candidates =[]
    for t, info in infos.items():
        if not info: continue
        
        # 規模與行業過濾
        if info.get('totalRevenue', 0) < 1_000_000_000: continue
        if any(ex.lower() in str(info.get('sector', '')).lower() for ex in EXCLUDED_INDUSTRIES): continue
        if info.get('marketCap', 0) == 0: continue
        
        # 財務與增長綜合評分 (FG Score: 營收增長 + 利潤率 + 自由現金流率)
        rev_growth = info.get('revenueGrowth', 0) or 0
        op_margin = info.get('operatingMargins', 0) or 0
        fcf = info.get('freeCashflow', 0) or 0
        rev = info.get('totalRevenue', 1)
        fcf_margin = fcf / rev if rev > 0 else 0
        
        # 若利潤率為負，直接淘汰
        if op_margin <= 0: continue
            
        fg_score = (rev_growth * 100) + (op_margin * 100) + (fcf_margin * 100)
        
        data = valid_technical_pool[t]
        data.update({
            "Ticker": t, "Industry": str(info.get('industry', ''))[:15], 
            "Market Cap": info.get('marketCap') / 1_000_000,
            "RS Rank": rs_ranks[t], "FG Score": fg_score
        })
        fundamental_candidates.append(data)

    # 【漏斗 3】：綜合排名高者入選 (只保留基本面最強的 Top 30)
    fundamental_candidates.sort(key=lambda x: x['FG Score'], reverse=True)
    top_tier_pool = fundamental_candidates[:30]

    # 【漏斗 4】：在最終頂尖候選池中，選取市值最小的 10 隻 (規則 6)
    # 如果 top_tier_pool 裡全被大盤股霸佔 (小盤股前面被淘汰了)，這裡就會選出 GOOGL
    top_10 = sorted(top_tier_pool, key=lambda x: x['Market Cap'])[:10]
    
    # 最終輸出時，按 RS Rank 降序，方便監控淘汰
    top_10.sort(key=lambda x: x['RS Rank'], reverse=True)

    # ==========================================
    # 3. 輸出至 Google Sheets (對齊 REL 視角)
    # ==========================================
    header = [["SuperGrowth Portfolio", f"更新: {datetime.datetime.now().strftime('%Y-%m-%d')}", "REL=相對SPY報酬", "", "", "", "", "", "", ""],["Ticker", "Industry", "Price", "Market Cap(M)", "RS Rank", "5D%", "20D%", "60D%", "REL 5", "REL 20", "REL 60"]
    ]
    
    final_list =[]
    for r in top_10:
        rs_str = f"⭐ {round(r['RS Rank'], 1)}" if r['RS Rank'] >= 90 else (f"⚠️ {round(r['RS Rank'], 1)}" if r['RS Rank'] < 80 else round(r['RS Rank'], 1))
        
        final_list.append([
            r['Ticker'], r['Industry'], round(r['Price'], 2), round(r['Market Cap'], 1),
            rs_str, 
            f"{round(r['5D%']*100, 2)}%", f"{round(r['20D%']*100, 2)}%", f"{round(r['60D%']*100, 2)}%",
            f"{round(r['REL 5']*100, 2)}%", f"{round(r['REL 20']*100, 2)}%", f"{round(r['REL 60']*100, 2)}%"
        ])

    sync_to_google_sheet(TARGET_SHEET, header + final_list)
    
    print("\n📊 入選 10 隻超級成長股預覽:")
    df_show = pd.DataFrame(final_list, columns=header[1])
    print(df_show.to_string(index=False))

if __name__ == "__main__":
    run_super_growth_funnel()
