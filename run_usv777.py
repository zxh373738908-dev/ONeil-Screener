import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import warnings
import math
import random

warnings.filterwarnings('ignore')

# ==========================================
# 1. 系統配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycby1pIM7iO43lcLQpOmi5LCJIn3VN9a0Ilf9amoy1EtQV_GBXJkk_A4PpsrJxKzH7i51/exec"

MONOPOLY_TICKERS =[
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "TSM", "ASML", "AVGO",
    "V", "MA", "BRK-B", "SPGI", "MCO", "LLY", "NVO", "UNH", "JNJ", "ISRG",       
    "WMT", "COST", "PG", "KO", "PEP", "LIN", "SHW", "CAT", "DE", "LMT",         
    "UNP", "WM", "RSG", "NOW", "SNPS", "CDNS"
]

FALLBACK_UNIVERSE = MONOPOLY_TICKERS +[
    "AMD", "CRWD", "PLTR", "PANW", "SNOW", "DDOG", "NET", "MDB", "TEAM", "WDAY",
    "ADBE", "CRM", "INTU", "QCOM", "TXN", "AMAT", "LRCX", "KLAC", "MU", "ARM",
    "UBER", "ABNB", "BKNG", "NFLX", "DIS", "CMCSA", "TMUS", "T",
    "XOM", "CVX", "COP", "EOG", "SLB", "GE", "RTX", "BA", "HON", "UPS"
]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'Credit Services']
MAX_PER_SECTOR = 4  # 提升單板塊上限以豐富 Top 排行榜

# ==========================================
# 2. 核心防禦工具 & API 裝甲與新增動能指標函數
# ==========================================
def sync_to_google_sheet(sheet_name, matrix):
    try:
        def safe_json_val(val):
            if isinstance(val, float) and not math.isfinite(val): return 0
            return str(val)
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=safe_json_val))}
        requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 同步成功 -> 分頁: [{sheet_name}]")
    except Exception as e: print(f"❌ 同步失敗[{sheet_name}]: {e}")

def safe_get(info_dict, key, default=0):
    val = info_dict.get(key)
    try: return float(val) if val is not None else default
    except: return default

def extract_ticker_data(data, ticker):
    try:
        if isinstance(data.columns, pd.MultiIndex):
            if ticker in data.columns.levels[0]: return data[ticker].dropna()
            elif ticker in data.columns.levels[1]: return data.xs(ticker, level=1, axis=1).dropna()
        return data.dropna()
    except: return pd.DataFrame()

def get_market_regime():
    try:
        spy_df = extract_ticker_data(yf.download("SPY", period="6mo", progress=False), "SPY") if "SPY" in yf.download("SPY", period="6mo", progress=False).columns else yf.download("SPY", period="6mo", progress=False)
        vix_df = extract_ticker_data(yf.download("^VIX", period="1mo", progress=False), "^VIX") if "^VIX" in yf.download("^VIX", period="1mo", progress=False).columns else yf.download("^VIX", period="1mo", progress=False)
        
        curr_spy, ma50_spy = float(spy_df['Close'].iloc[-1]), float(spy_df['Close'].tail(50).mean())
        curr_vix = float(vix_df['Close'].iloc[-1])
        return (curr_spy > ma50_spy) and (curr_vix < 22), curr_vix
    except: return True, 15.0

# 新增：安全計算回報率 (Return)
def safe_ret(series, periods):
    try:
        if len(series) > periods:
            return (float(series.iloc[-1]) / float(series.iloc[-(periods+1)]) - 1) * 100
    except: pass
    return 0

# 新增：安全計算相對大盤強度 (Relative Strength vs SPY)
def safe_rel(series, spy_series, periods):
    try:
        if len(series) > periods and len(spy_series) > periods:
            stock_ret = float(series.iloc[-1]) / float(series.iloc[-(periods+1)])
            spy_ret = float(spy_series.iloc[-1]) / float(spy_series.iloc[-(periods+1)])
            return (stock_ret / spy_ret - 1) * 100 # 超額報酬率
    except: pass
    return 0

# 新增：計算自特定日期起的 YTD 報酬 (From 2025-12-31)
def ytd_ret(series, date_str='2025-12-31'):
    try:
        past_series = series.loc[:date_str]
        if not past_series.empty:
            return (float(series.iloc[-1]) / float(past_series.iloc[-1]) - 1) * 100
    except: pass
    return 0

# ==========================================
# 3. 🛡️ 策略 A: 左側黃金坑
# ==========================================
def run_left_side_golden_pit(curr_vix):
    print("\n" + "="*50 + "\n🛡️[策略 A: 左側黃金坑] 啟動掃描...")
    
    try: data = yf.download(MONOPOLY_TICKERS, period="5y", interval="1d", group_by='ticker', auto_adjust=True, progress=False)
    except: return

    candidates =[]
    for t in MONOPOLY_TICKERS:
        try:
            df = extract_ticker_data(data, t)
            if len(df) < 250: continue
            
            close, vol = df['Close'], df['Volume']
            curr_price, ma20 = float(close.iloc[-1]), close.tail(20).mean()
            
            df_w = df.resample('W-FRI').agg({'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
            drawdown = (curr_price - float(df_w['High'].max())) / float(df_w['High'].max())
            
            if drawdown > -0.30 or curr_price < ma20: continue 
            if vol.tail(20).max() < (vol.tail(60).mean() * 1.5): continue 
            
            time.sleep(0.2)
            info = yf.Ticker(t).info
            if safe_get(info, 'freeCashflow') > 0 and safe_get(info, 'revenueGrowth') > -0.10:
                discipline = "翻倍平半 / 零止損" if curr_vix < 25 else "⚠️IV極高！改做 Bull Call Spread"
                candidates.append({
                    "Ticker": t, "Price": curr_price, "Drawdown": drawdown*100,
                    "Status": "FCF健康 | 20MA企穩 | 📈放量", "Strike": curr_price * 0.80, "Discipline": discipline
                })
        except: continue

    header =[["🛡️ 左側黃金坑", "更新:", datetime.datetime.now().strftime('%m-%d %H:%M'), "VIX:", f"{curr_vix:.1f}", "", "", ""],["代碼", "現價", "5年跌幅", "底部信號", "基本面狀態", "建議合約", "到期日", "交易紀律"]
    ]
    final_list = [["-", "市場處於高位，耐心等待", "-", "-", "-", "-", "-", "-"]] if not candidates else [
        [r['Ticker'], round(r['Price'], 2), f"{round(r['Drawdown'], 2)}%", "爆量+20MA企穩", r['Status'], 
         f"Deep ITM Call @ ${round(r['Strike'], 2)}", "> 360 Days", r['Discipline']]
        for r in sorted(candidates, key=lambda x: x['Drawdown'])
    ]
    sync_to_google_sheet("🛡️左側_黃金坑", header + final_list)

# ==========================================
# 4. 🚀 策略 B: 右側全景動能成長 (包含新增進階指標)
# ==========================================
def run_right_side_momentum(is_bull, curr_vix):
    print("\n" + "="*50 + "\n🚀 [策略 B: 右側動能成長 (全景數據版)] 啟動掃描...")
    
    market_status = f"🟢 允許交易 (VIX:{curr_vix:.1f})" if is_bull else f"🔴 轉弱或恐慌 (VIX:{curr_vix:.1f})，暫停"
    
    # 全新擴充的 Header 結構
    header = [["🚀 動能成長 Top 20", "更新:", datetime.datetime.now().strftime('%m-%d %H:%M'), "大盤狀態:", market_status, "", "", "", "", "", "", "", "", "", "", "", "", "", ""],["Rank", "Ticker", "Sector", "Price", "1D%", "Bias (50MA)", "60-Day Trend", "R20", "R60", "R120", "REL5", "REL20", "REL60", "REL120", "From 2025-12-31", "RS Score", "Msg", "Total Score", "Action"]
    ]

    if not is_bull:
        sync_to_google_sheet("🚀右側_動能成長", header + [["-", "環境惡劣", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "保留現金", "-", "嚴禁追高"]])
        return

    try: tickers =[t.replace('.', '-') for t in pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]['Symbol'].tolist()]
    except: tickers = FALLBACK_UNIVERSE
    
    # 將 SPY 強制加入以取得 REL 的基準基準資料
    if "SPY" not in tickers: tickers.append("SPY")

    print("📡 下載股票池與 SPY K 線...")
    try: data = yf.download(tickers, period="1y", interval="1d", group_by='ticker', auto_adjust=True, threads=True, progress=False)
    except: return

    # 提取 SPY 收盤資料供後續比較使用
    try:
        spy_df = extract_ticker_data(data, "SPY")
        spy_close = spy_df['Close'] if 'Close' in spy_df else pd.Series()
    except: spy_close = pd.Series()

    cands =[]
    for t in tickers:
        if t == "SPY": continue
        try:
            df = extract_ticker_data(data, t)
            if len(df) < 100: continue
            
            close, vol = df['Close'], df['Volume']
            curr_price = float(close.iloc[-1])
            ma50 = close.tail(50).mean()
            
            if curr_price < ma50 or ((curr_price - ma50) / ma50) > 0.35: continue
            
            ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
            dist_to_ema20 = abs(curr_price - ema20) / ema20
            in_sniper_zone = dist_to_ema20 <= 0.025 
            
            rvol = float(vol.iloc[-1] / vol.tail(10).mean())
            risk_pct = ((curr_price - ma50) / curr_price) * 100 
            
            # --- 【全新全景指標計算區】 ---
            r1d = safe_ret(close, 1)
            r20 = safe_ret(close, 20)
            r60 = safe_ret(close, 60)
            r120 = safe_ret(close, 120)
            r250 = safe_ret(close, 250)
            
            # Bias (乖離率) 設為相對 50MA 的距離
            bias = ((curr_price - ma50) / ma50) * 100
            
            # 60-Day Trend：我們測算 60日線 近一個月的斜率走勢
            ma60 = close.rolling(60).mean().dropna()
            trend_60d = (float(ma60.iloc[-1]) / float(ma60.iloc[-20]) - 1) * 100 if len(ma60) >= 20 else 0
            
            # REL (相對 SPY 動能) 計算
            rel5 = safe_rel(close, spy_close, 5)
            rel20 = safe_rel(close, spy_close, 20)
            rel60 = safe_rel(close, spy_close, 60)
            rel120 = safe_rel(close, spy_close, 120)
            
            # 特定基準日 (2025-12-31) 到今日的漲幅
            ytd_2026 = ytd_ret(close, '2025-12-31')
            
            # RS 傳統綜合得分更新為使用百分比權重計算
            rs_score = (r20/100 * 0.4) + (r60/100 * 0.3) + (r250/100 * 0.3)
            
            if rs_score > 0:
                cands.append({
                    "Ticker": t, "Price": curr_price, "RS": rs_score, 
                    "1D%": r1d, "R20": r20, "R60": r60, "R120": r120,
                    "Bias": bias, "60D_Trend": trend_60d,
                    "REL5": rel5, "REL20": rel20, "REL60": rel60, "REL120": rel120,
                    "YTD": ytd_2026,
                    "Tightness": (close.tail(15).std() / close.tail(15).mean()) * 100, 
                    "RVOL": rvol, "Sniper": in_sniper_zone, "Risk_Pct": risk_pct
                })
        except: continue

    cands = sorted(cands, key=lambda x: x['RS'], reverse=True)[:50] 
    print(f"🔬 體檢 (共 {len(cands)} 隻)，執行均衡與風險測算...")
    
    final_cands =[]
    for c in cands:
        try:
            time.sleep(random.uniform(0.1, 0.3)) 
            info = yf.Ticker(c['Ticker']).info
            
            sec, ind = str(info.get('sector', '')), str(info.get('industry', ''))
            if any(ex.lower() in sec.lower() or ex.lower() in ind.lower() for ex in EXCLUDED_INDUSTRIES): continue
            
            fin_score, fin_msg = 0, ""
            rev_growth = safe_get(info, 'revenueGrowth') * 100
            
            if 'Technology' in sec or 'Software' in ind:
                r40 = rev_growth + ((safe_get(info, 'freeCashflow') / (safe_get(info, 'totalRevenue', 1) or 1)) * 100)
                if r40 >= 30.0: fin_score, fin_msg = r40, f"Rule 40 ({r40:.0f}%)"
            else:
                op_margin = safe_get(info, 'operatingMargins') * 100
                if op_margin >= 10.0 and rev_growth > -5.0: fin_score, fin_msg = op_margin + rev_growth, f"利潤 ({op_margin:.1f}%)"

            if fin_score > 0:
                tight_msg = f" | 收斂:{c['Tightness']:.1f}%" if c['Tightness'] < 4.0 else ""
                vol_msg = f" 📈{c['RVOL']:.1f}x量" if c['RVOL'] > 1.2 else ""
                
                c['Sec'] = sec.replace("Consumer Defensive", "Cons Def").replace("Consumer Cyclical", "Cons Cyc")[:12]
                c['Msg'] = fin_msg + tight_msg + vol_msg
                c['Tot'] = (c['RS']*100) + fin_score - (c['Tightness'] * 1.5)
                
                final_cands.append(c)
        except: continue

    # 取最高分 Top 20 (各板塊最多保留 MAX_PER_SECTOR 支)
    sorted_all = sorted(final_cands, key=lambda x: x['Tot'], reverse=True)
    top_cands, sector_counts =[], {}
    
    for r in sorted_all:
        sec = r['Sec']
        if sector_counts.get(sec, 0) < MAX_PER_SECTOR:
            top_cands.append(r)
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if len(top_cands) >= 20: break 

    # 封裝所有數據維度至 Google Sheet 列表格式
    final_list = [["-"] * 19] if not top_cands else [[
         f"Top {i+1}",                               # Rank
         r['Ticker'],                                # Ticker
         r['Sec'],                                   # Sector
         round(r['Price'], 2),                       # Price
         f"{round(r['1D%'], 2)}%",                   # 1D%
         f"{round(r['Bias'], 2)}%",                  # Bias
         f"{round(r['60D_Trend'], 2)}%",             # 60-Day Trend
         f"{round(r['R20'], 2)}%",                   # R20
         f"{round(r['R60'], 2)}%",                   # R60
         f"{round(r['R120'], 2)}%",                  # R120
         f"{round(r['REL5'], 2)}%",                  # REL5
         f"{round(r['REL20'], 2)}%",                 # REL20
         f"{round(r['REL60'], 2)}%",                 # REL60
         f"{round(r['REL120'], 2)}%",                # REL120
         f"{round(r['YTD'], 2)}%",                   # From 2025-12-31
         round(r['RS']*100, 2),                      # RS Score
         r['Msg'],                                   # 基本面 Msg
         round(r['Tot'], 2),                         # Total Score
         f"🎯 回踩買入 (止損風險: -{r['Risk_Pct']:.1f}%)" if r['Sniper'] else f"等待回踩 20EMA (風險: -{r['Risk_Pct']:.1f}%)"
        ]
        for i, r in enumerate(top_cands)
    ]
    sync_to_google_sheet("🚀右側_動能成長", header + final_list)

if __name__ == "__main__":
    print("🌟 啟動【雙引擎量化系統 V11.0 - 動能全景矩陣版】...")
    is_bull, curr_vix = get_market_regime()
    run_left_side_golden_pit(curr_vix)
    run_right_side_momentum(is_bull, curr_vix)
    print("\n✅ 所有策略執行完畢！")
