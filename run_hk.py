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
# 1. 基础设置与 Google Sheets 连接
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
# 🧠 核心算法：筹码分布 POC 计算 (模拟 TradingView)
# ==========================================
def calculate_poc_hk(df, bins=50):
    """
    计算过去120个交易日的 POC (Point of Control)
    """
    if len(df) < 60: return 0.0, 0.0
    
    # 取最近半年数据
    lookback_df = df.tail(120)
    p_min = lookback_df['Low'].min()
    p_max = lookback_df['High'].max()
    
    if p_max == p_min: return 0.0, 0.0
    
    # 使用直方图计算筹码分布
    counts, bin_edges = np.histogram(lookback_df['Close'], bins=bins, weights=lookback_df['Volume'])
    
    max_bin_idx = np.argmax(counts)
    poc_price = (bin_edges[max_bin_idx] + bin_edges[max_bin_idx+1]) / 2
    
    current_price = df['Close'].iloc[-1]
    dist_to_poc = (current_price - poc_price) / poc_price
    
    return round(poc_price, 2), dist_to_poc

# ==========================================
# 🌐 附加模块：腾讯证券极速汉化引擎
# ==========================================
def translate_to_chinese_via_tencent(df):
    print("   -> 🌐 正在调取【腾讯证券】主节点进行极速汉化...")
    codes = df['代码'].tolist()
    cn_mapping = {}
    
    chunk_size = 50
    for i in range(0, len(codes), chunk_size):
        chunk = codes[i:i+chunk_size]
        query_str = ",".join([f"hk{str(c).zfill(5)}" for c in chunk])
        url = f"http://qt.gtimg.cn/q={query_str}"
        try:
            resp = requests.get(url, timeout=5)
            matches = re.findall(r'v_hk(\d+)="[^~]+~([^~]+)~', resp.text)
            for code, name in matches:
                clean_code = str(code).lstrip('0')
                cn_mapping[clean_code] = name
        except: pass
            
    df['名称'] = df.apply(lambda row: cn_mapping.get(str(row['代码']), row['名称']), axis=1)
    return df

# ==========================================
# 🌍 STEP 1: 获取港股名册 (TradingView)
# ==========================================
def get_hk_share_list():
    print("\n🌍 [STEP 1] 连接 TradingView 提取百亿级港股名册...")
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {
        "columns": ["name", "description", "close", "market_cap_basic"],
        "range": [0, 4000], 
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "filter": [{"left": "type", "operation": "equal", "right": "stock"}]
    }
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
    
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=20)
        data = resp.json()
        stock_list = []
        for item in data.get("data", []):
            fields = item.get("d", [])
            if len(fields) >= 4:
                clean_sym = re.sub(r'[^0-9]', '', str(fields[0]))
                if not clean_sym: continue
                stock_list.append({
                    "代码": clean_sym,
                    "名称": fields[1],
                    "最新价": float(fields[2]) if fields[2] else 0.0,
                    "总市值": float(fields[3]) if fields[3] else 0.0
                })
        df = pd.DataFrame(stock_list)
        # 过滤：股价>1 且 市值>100亿
        df = df[(df['最新价'] >= 1.0) & (df['总市值'] >= 10000000000)].copy()
        df = translate_to_chinese_via_tencent(df)
        print(f"   -> ✅ 提纯出 {len(df)} 只优质标的。")
        return df
    except Exception as e:
        print(f"   -> ❌ 错误: {e}")
        return pd.DataFrame()

# ==========================================
# 🧠 STEP 2: 核心选股引擎 (加入筹码 POC 逻辑)
# ==========================================
def apply_advanced_logic(ticker, name, df_hist, mktcap):
    if len(df_hist) < 200: return {"status": "fail", "reason": "数据不足"}

    closes = df_hist['Close'].values
    highs = df_hist['High'].values
    lows = df_hist['Low'].values
    vols = df_hist['Volume'].values
    
    close = closes[-1]
    if close == 0.0 or vols[-1] == 0: return {"status": "fail", "reason": "停牌"}

    # --- 筹码 POC 计算 ---
    poc_price, dist_to_poc = calculate_poc_hk(df_hist)

    # --- 基础指标 ---
    ma20 = np.mean(closes[-20:])
    ma50 = np.mean(closes[-50:])
    ma200 = np.mean(closes[-200:])
    h250 = np.max(highs[-250:])
    
    avg_v50 = np.mean(vols[-50:])
    vol_ratio = vols[-1] / avg_v50 if avg_v50 > 0 else 0
    pct_chg = (close - closes[-2]) / closes[-2] if closes[-2] > 0 else 0

    # RSI
    deltas = np.diff(closes[-15:])
    up = pd.Series(np.where(deltas > 0, deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
    down = pd.Series(np.where(deltas < 0, -deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
    rsi = 100 - (100 / (1 + (up/down))) if down > 0 else 100

    # 🌟 战术 1：老龙回头 + 筹码支撑 (极品共振)
    # 股价靠近 MA20/MA50，且靠近 POC 支撑线
    is_at_ma_support = (abs(close - ma20)/ma20 < 0.04) or (abs(close - ma50)/ma50 < 0.04)
    is_at_poc_support = (dist_to_poc >= -0.01) and (dist_to_poc <= 0.06)
    
    is_dragon_return = (ma50 > ma200) and is_at_ma_support and is_at_poc_support and (rsi < 60)

    # 🚀 战术 2：放量起爆 (突破筹码峰)
    is_breakout = (vol_ratio > 1.8) and (pct_chg > 0.03) and (close > poc_price) and (close > ma20)

    # 📈 战术 3：趋势多头
    is_uptrend = (close > ma20 > ma50 > ma200) and (rsi > 55)

    if not (is_dragon_return or is_breakout or is_uptrend):
        return {"status": "fail", "reason": "形态未达标"}

    trend_tag = []
    if is_dragon_return: trend_tag.append("🐉老龙回头(筹码共振)")
    if is_breakout: trend_tag.append("🚀放量突破")
    if is_uptrend: trend_tag.append("📈趋势多头")

    # 计算 60 日涨幅用于排序
    ret_60 = (close - closes[-61]) / closes[-61] if len(closes) > 61 else 0

    return {
        "status": "success",
        "data": {
            "代码": ticker.replace(".HK", ""),
            "名称": name,
            "现价": round(close, 2),
            "POC支撑": poc_price,
            "距POC%": f"{round(dist_to_poc*100, 2)}%",
            "60日涨幅%": round(ret_60 * 100, 2),
            "RSI": round(rsi, 2),
            "量比": round(vol_ratio, 2),
            "市值(亿)": round(mktcap / 100000000, 2),
            "形态标签": " + ".join(trend_tag)
        }
    }

# ==========================================
# 🚀 STEP 3: 批量演算
# ==========================================
def scan_hk_market(df_list):
    print(f"\n🚀 [STEP 2] 启动 Yahoo 演算引擎，处理 {len(df_list)} 只标的...")
    tickers = [str(c).lstrip('0').zfill(4) + '.HK' for c in df_list['代码']]
    ticker_info = {row['代码'].lstrip('0').zfill(4) + '.HK': row for _, row in df_list.iterrows()}
    
    all_results = []
    chunk_size = 100
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
        
        for t in chunk:
            try:
                df_hist = data.xs(t, axis=1, level=1).dropna() if len(chunk) > 1 else data.dropna()
                if df_hist.empty: continue
                
                res = apply_advanced_logic(t, ticker_info[t]['名称'], df_hist, ticker_info[t]['总市值'])
                if res["status"] == "success":
                    all_results.append(res["data"])
            except: continue
            
    return all_results

# ==========================================
# 📝 STEP 4: 写入 Google Sheets
# ==========================================
def write_sheet(results):
    print("\n📝 [STEP 3] 正在同步作战指令至 Google Sheets...")
    sheet = get_worksheet("HK-Chip-Screener")
    sheet.clear()
    
    if not results:
        sheet.update_acell("A1", "今日无符合筹码战法标的。")
        return

    df = pd.DataFrame(results)
    # 按 60 日涨幅降序
    df = df.sort_values(by='60日涨幅%', ascending=False)
    
    # 写入数据
    header = df.columns.tolist()
    values = [header] + df.values.tolist()
    sheet.update(values=values, range_name="A1")
    
    # 时间戳
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    sheet.update_acell("L1", "最后更新时间:")
    sheet.update_acell("M1", now_str)
    print(f"🎉 成功！{len(df)} 只标的已装填。")

# ==========================================
# 主函数
# ==========================================
if __name__ == "__main__":
    try:
        # 1. 获取名单
        base_df = get_hk_share_list()
        if not base_df.empty:
            # 2. 扫描逻辑
            final_res = scan_hk_market(base_df)
            # 3. 写入表格
            write_sheet(final_res)
    except Exception as e:
        print(f"致命错误: {e}")
        traceback.print_exc()
