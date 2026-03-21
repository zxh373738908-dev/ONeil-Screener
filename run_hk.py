import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import traceback
import yfinance as yf
import logging
import requests
import re
from collections import defaultdict

warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 基礎設置與 Google Sheets 連接
# ==========================================
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def get_worksheet(sheet_name="HK-Share Screener"):
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    try:
        return doc.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        return doc.add_worksheet(title=sheet_name, rows=100, cols=20)

# ==========================================
# 🌐 附加模塊：騰訊證券極速漢化引擎
# ==========================================
def translate_to_chinese_via_tencent(df):
    print(" -> 🌐 正在調取【騰訊證券】主節點，進行股票名稱極速漢化...")
    codes = df['代碼'].tolist()
    cn_mapping = {}
    chunk_size = 50
    for i in range(0, len(codes), chunk_size):
        chunk = codes[i:i+chunk_size]
        query_str = ",".join([f"hk{str(c).zfill(5)}" for c in chunk])
        url = f"http://qt.gtimg.cn/q={query_str}"
        try:
            resp = requests.get(url, timeout=5)
            matches = re.findall(r'v_hk(\d+)="[^~]+~([^~]+)', resp.text)
            for code, name in matches:
                clean_code = str(code).lstrip('0')
                cn_mapping[clean_code] = name
        except Exception:
            pass
    df['名稱'] = df.apply(lambda row: cn_mapping.get(str(row['代碼']), row['名稱']), axis=1)
    print(f" -> ✅ 騰訊數據橋接成功！已完美漢化 {len(cn_mapping)} 隻核心標的。")
    return df

# ==========================================
# 📊 核心升級：Python 原生 FRVP 籌碼峰演算引擎
# ==========================================
def calculate_frvp_poc(highs, lows, vols, lookback=120, bins=50):
    """
    模擬 TradingView 的 Fixed Range Volume Profile
    精準計算過去 N 天的 POC (Point of Control) 主力成本線
    """
    if len(highs) < lookback:
        lookback = len(highs)
        
    recent_h = highs[-lookback:]
    recent_l = lows[-lookback:]
    recent_v = vols[-lookback:]
    
    min_p, max_p = np.min(recent_l), np.max(recent_h)
    if max_p == min_p: 
        return min_p
        
    bin_size = (max_p - min_p) / bins
    profile = np.zeros(bins)
    
    # 將每一天的成交量按比例分配到對應的價格區間(Bin)中
    for h, l, v in zip(recent_h, recent_l, recent_v):
        if v == 0 or np.isnan(v): continue
        start_bin = max(0, int((l - min_p) / bin_size))
        end_bin = min(bins - 1, int((h - min_p) / bin_size))
        
        if start_bin == end_bin:
            profile[start_bin] += v
        else:
            vol_per_bin = v / (end_bin - start_bin + 1)
            for b in range(start_bin, end_bin + 1):
                profile[b] += vol_per_bin
                
    # 找出成交量最大的價格區間 (POC)
    poc_bin = np.argmax(profile)
    poc_price = min_p + (poc_bin + 0.5) * bin_size
    return poc_price

# ==========================================
# 🌍 STEP 1: 獲取港股名冊 (新增讀取 TV 的 VWAP 數據)
# ==========================================
def get_hk_share_list():
    print("\n🌍 [STEP 1] 啟動【底層脫殼機制】：切換至國際級【TradingView 量化中樞】...")
    url = "https://scanner.tradingview.com/hongkong/scan"
    # ⚠️ 優化：直接讓 TradingView 吐出 VWAP (日內成交量加權平均價)
    payload = {
        "columns": ["name", "description", "close", "market_cap_basic", "VWAP"],
        "range": [0, 4000],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "filter": [{"left": "type", "operation": "equal", "right": "stock"}]
    }
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
    
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=20)
        data = resp.json()
        raw_list = data.get("data", [])
        stock_list = []
        for item in raw_list:
            fields = item.get("d", [])
            if len(fields) >= 5:
                raw_code, name_eng, price, mktcap, vwap = fields[0], fields[1], fields[2], fields[3], fields[4]
                clean_sym = re.sub(r'[^0-9]', '', str(raw_code))
                if not clean_sym: continue
                stock_list.append({
                    "代碼": clean_sym,
                    "名稱": name_eng,
                    "最新價": float(price) if price else 0.0,
                    "總市值": float(mktcap) if mktcap else 0.0,
                    "VWAP": float(vwap) if vwap else 0.0  # 保存日內 VWAP 防禦線
                })
        df = pd.DataFrame(stock_list)
    except Exception as e:
        print(f" -> ❌ 致命錯誤：TradingView 數據流被阻斷: {e}")
        return pd.DataFrame()

    df = df[(df['最新價'] >= 1.0) & (df['總市值'] >= 10000000000)].copy()
    df = df[df['名稱'].astype(str).str.strip() != '']
    df = translate_to_chinese_via_tencent(df)
    return df[['代碼', '名稱', '最新價', '總市值', 'VWAP']]

# ==========================================
# 🧠 STEP 2: 籌碼峰優化選股引擎 (上帝視角)
# ==========================================
def apply_advanced_logic(ticker, name, opens, closes, highs, lows, vols, amounts, mktcap, tv_vwap):
    if len(closes) < 250: return {"status": "fail", "reason": "次新/數據不足250天"}
    close = closes[-1]
    last_amount = amounts[-1]
    avg_amount_5d = np.mean(amounts[-5:])
    
    if close == 0.0 or vols[-1] == 0: return {"status": "fail", "reason": "停牌/今日無數據"}
    if avg_amount_5d < 50000000: return {"status": "fail", "reason": "5日均成交萎靡(<5000萬)"}

    # 基礎指標
    ma20 = np.mean(closes[-20:])
    ma50 = np.mean(closes[-50:])
    ma150 = np.mean(closes[-150:])
    ma200 = np.mean(closes[-200:])
    h250 = np.max(highs[-250:])
    
    avg_v50 = np.mean(vols[-50:])
    vol_ratio_today = vols[-1] / avg_v50 if avg_v50 > 0 else 0
    pct_change_today = (close - closes[-2]) / closes[-2] if closes[-2] > 0 else 0
    
    # 🎯 核心武器：計算過去半年的籌碼峰 POC (主力絕對成本線)
    poc_6m = calculate_frvp_poc(highs, lows, vols, lookback=120, bins=60)
    
    # 計算今日收盤與 POC 的距離
    dist_to_poc = (close - poc_6m) / poc_6m if poc_6m > 0 else 0

    # 🌟 戰役一：籌碼真空拔槍點 (跳空越過雷區)
    # 條件：放量突破，且價格剛剛站上整個籌碼山峰的最長紅線 (POC) 之上 (<5%)，進入真空區！
    is_vacuum_breakout = (
        (vol_ratio_today >= 1.5) and 
        (pct_change_today >= 0.03) and 
        (close > poc_6m) and 
        (dist_to_poc < 0.05) and # 剛好突破 POC
        (close >= h250 * 0.70)
    )

    # 🌟 戰役二：均線+籌碼 雙重神級共振底 (老龍回頭極致版)
    # 條件：POC 與 50 日均線重合 (誤差<3%)，且價格在雙重防禦線上企穩縮量
    poc_ma50_confluence = abs(poc_6m - ma50) / ma50 <= 0.03
    touch_confluence = abs(close - poc_6m) / poc_6m <= 0.03
    
    is_confluence_dip = (
        (ma50 > ma150) and 
        poc_ma50_confluence and 
        touch_confluence and 
        (vol_ratio_today < 1.2) # 洗盤縮量
    )

    # ================= 裁決邏輯 =================
    if not (is_vacuum_breakout or is_confluence_dip):
        return {"status": "fail", "reason": "未觸發籌碼峰極品作戰形態"}

    # 日內防禦：如果收盤價跌破了 TV 提供的今日 VWAP，說明日內主力被套，一票否決！
    if tv_vwap > 0 and close < tv_vwap and not is_confluence_dip:
        return {"status": "fail", "reason": "跌破日內 VWAP 防禦線"}

    trend_tag = []
    if is_confluence_dip: trend_tag.append("🛡️神級共振底(POC+MA50)")
    if is_vacuum_breakout: trend_tag.append("🚀籌碼真空區突破(主力拉升)")

    data = {
        "Ticker": ticker.replace(".HK", ""),
        "Name": name,
        "Price": round(close, 2),
        "POC(半年)": round(poc_6m, 2), # 顯示主力成本線
        "VWAP(日內)": round(tv_vwap, 2), # 顯示日內成本線
        "Dist_POC(%)": round(dist_to_poc * 100, 2),
        "Vol_Ratio": round(vol_ratio_today, 2),
        "Mkt_Cap(億)": round(mktcap / 100000000, 2),
        "Turnover(億)": round(last_amount / 100000000, 2),
        "Trend": " + ".join(trend_tag)
    }
    return {"status": "success", "data": data}

# ==========================================
# 🚀 STEP 3: Yahoo Finance 併發演算
# ==========================================
def scan_hk_market_via_yfinance(df_list):
    print("\n🚀 [STEP 2] 啟動【Yahoo + FRVP 籌碼峰】天基武器，執行高維矩陣演算...")
    tickers = []
    ticker_to_info = {}
    
    for _, row in df_list.iterrows():
        code = str(row['代碼'])
        yf_code = code.lstrip('0').zfill(4) + '.HK'
        tickers.append(yf_code)
        ticker_to_info[yf_code] = {
            'name': row['名稱'],
            'mktcap': row['總市值'],
            'vwap': row['VWAP']
        }

    all_results = []
    fail_reasons = defaultdict(int)
    chunk_size = 500
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f" -> 📥 正在下載並渲染籌碼峰 第 {i+1} ~ {min(i+chunk_size, len(tickers))} 隻標的...")
        data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
        
        for ticker in chunk:
            try:
                if len(chunk) > 1:
                    closes = data['Close'][ticker].dropna().values
                    opens = data['Open'][ticker].dropna().values
                    highs = data['High'][ticker].dropna().values
                    lows = data['Low'][ticker].dropna().values
                    vols = data['Volume'][ticker].dropna().values
                else:
                    closes = data['Close'].dropna().values
                    opens = data['Open'].dropna().values
                    highs = data['High'].dropna().values
                    lows = data['Low'].dropna().values
                    vols = data['Volume'].dropna().values
                    
                if len(closes) < 120:
                    fail_reasons["次新/數據不足(<120天)"] += 1
                    continue
                    
                amounts = closes * vols
                info = ticker_to_info[ticker]
                res = apply_advanced_logic(ticker, info['name'], opens, closes, highs, lows, vols, amounts, info['mktcap'], info['vwap'])
                
                if res["status"] == "success":
                    all_results.append(res["data"])
                else:
                    fail_reasons[res["reason"]] += 1
            except Exception:
                fail_reasons["數據異常截斷"] += 1
                continue
                
    return all_results, fail_reasons

# ==========================================
# 📝 STEP 4: 寫入作戰指令
# ==========================================
def write_sheet(final_stocks, diag_msg=None):
    print("\n📝 [STEP 3] 正在將【上帝視角】作戰名單寫入 Google Sheets...")
    sheet_name = "HK-Share Screener"
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        sheet = get_worksheet(sheet_name)
        sheet.clear()
        if len(final_stocks) == 0:
            sheet.update_acell("A1", "No Signal: 未發現符合籌碼共振/真空突破的極品。")
            if diag_msg: sheet.update_acell("A3", diag_msg)
            return
            
        df = pd.DataFrame(final_stocks)
        # 優先按照距離 POC 的突破完美度排序
        df = df.sort_values(by='Dist_POC(%)', ascending=True)
        df = df.head(50)
        
        sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
        sheet.update_acell("M1", "Last Updated(UTC+8):")
        sheet.update_acell("N1", now_str)
        if diag_msg: sheet.update_acell("O1", diag_msg)
        print(f"🎉 大功告成！已成功將 {len(df)} 隻【籌碼峰認證】龍頭送達指揮部！")
    except Exception as e:
        print(f"❌ 表格寫入失敗: {e}")

# ==========================================
# MAIN 主函數
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n========== 港股獵手系統 V5.0 (籌碼峰上帝視角版) ==========")
    print(f"⏰ 當前系統時間 (UTC+8): {now_str}")
    
    df_list = get_hk_share_list()
    if df_list.empty: return
    
    final_stocks, fail_reasons = scan_hk_market_via_yfinance(df_list)
    
    fail_str = "".join([f" - {r}: {c} 隻\n" for r, c in sorted(fail_reasons.items(), key=lambda x:x[1], reverse=True)])
    diag_msg = (
        f"[{now_str}] 籌碼峰矩陣掃描完畢：\n"
        f"📊 百億基礎過濾池: {len(df_list)}隻\n"
        f"🏆 籌碼真空/神級共振: {len(final_stocks)}隻\n"
        f"🔪 淘汰明細：\n{fail_str}"
    )
    print("\n" + diag_msg)
    write_sheet(final_stocks, diag_msg=diag_msg)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ 系統發生致命異常:\n{traceback.format_exc()}")
