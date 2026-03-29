import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import yfinance as yf
import requests
import re
import time
from gspread_formatting import *

# 基础设置
warnings.filterwarnings('ignore')

# ==========================================
# 1. 核心配置
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_GID = 665566258  
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_v32_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    for ws in doc.worksheets():
        if ws.id == TARGET_GID:
            return ws
    return doc.get_worksheet(0)

# ==========================================
# 🌐 2. 数据工具
# ==========================================
def get_chinese_names(codes):
    mapping = {}
    chunk_size = 50
    for i in range(0, len(codes), chunk_size):
        chunk = [f"hk{str(c).zfill(5)}" for c in codes[i:i+chunk_size]]
        url = f"http://qt.gtimg.cn/q={','.join(chunk)}"
        try:
            r = requests.get(url, timeout=5)
            matches = re.findall(r'v_hk(\d+)="[^~]+~([^~]+)', r.text)
            for c, n in matches: mapping[str(c).lstrip('0')] = n
        except: pass
    return mapping

# ==========================================
# 🧠 3. 演算核心
# ==========================================

def calculate_frvp_poc(highs, lows, vols, lookback=120):
    if len(vols) < lookback: lookback = len(vols)
    h_s, l_s, v_s = highs[-lookback:], lows[-lookback:], vols[-lookback:]
    min_p, max_p = np.min(l_s), np.max(h_s)
    if max_p == min_p: return min_p
    bins = 50
    bin_size = (max_p - min_p) / bins
    profile = np.zeros(bins)
    for i in range(lookback):
        s_bin = max(0, int((l_s[i] - min_p) / bin_size))
        e_bin = min(bins - 1, int((h_s[i] - min_p) / bin_size))
        profile[s_bin:e_bin+1] += v_s[i] / (e_bin - s_bin + 1)
    return min_p + (np.argmax(profile) + 0.5) * bin_size

def analyze_v32(ticker, name, df_h, mkt_cap, tv_turnover, tv_vwap, hsi_series):
    if df_h.empty or len(df_h) < 250: return None
    try:
        close = df_h['Close'].values.flatten()
        high = df_h['High'].values.flatten()
        low = df_h['Low'].values.flatten()
        vol = df_h['Volume'].values.flatten()
        cp = close[-1]
    except: return None

    if np.isnan(cp): return None

    ma50, ma200 = np.mean(close[-50:]), np.mean(close[-200:])
    if cp < ma200: return None 

    poc_6m = calculate_frvp_poc(high, low, vol)
    dist_poc = (cp / poc_6m - 1) * 100
    
    # RS 原始分
    rs_raw = (cp / close[-250]) / (hsi_series.iloc[-1] / hsi_series.iloc[-250])

    avg_vol50 = np.mean(vol[-50:])
    vol_ratio = vol[-1] / avg_vol50 if avg_vol50 > 0 else 0
    ret_60d = (cp / close[-60] - 1) * 100
    
    # RSI
    delta = np.diff(close[-20:])
    gain = np.mean(delta[delta > 0]) if any(delta > 0) else 0.001
    loss = -np.mean(delta[delta < 0]) if any(delta < 0) else 0.001
    rsi = 100 - (100 / (1 + gain/loss))

    trend_tag = "📈經典多頭(逼近新高)"
    if abs(cp - poc_6m)/poc_6m < 0.03 and vol[-1] < avg_vol50 * 0.7:
        trend_tag = "🐉老龍回頭(👑籌碼共振)"
    elif cp > poc_6m and vol_ratio > 1.8:
        trend_tag = "🚀放量起爆(🌪️躍入真空區)"

    return {
        "Ticker": ticker.replace(".HK", ""), "Name": name, "Price": round(float(cp), 2),
        "POC": round(float(poc_6m), 2), "VWAP": round(float(tv_vwap), 2), "Dist_POC": round(float(dist_poc), 2),
        "60D_Ret": round(float(ret_60d), 2), "RSI": round(float(rsi), 2), "Vol_Ratio": round(float(vol_ratio), 2),
        "Mkt_Cap": round(float(mkt_cap/1e8), 2), "Turnover": round(float(tv_turnover/1e8), 2),
        "Trend": trend_tag, "rs_raw": rs_raw
    }

# ==========================================
# 🚀 4. 主执行流程
# ==========================================

def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 V32.2 Alpha 猎手启动 (修复 TextFormat 属性)...")
    
    # 1. 恒指基准
    hsi_raw = yf.download("^HSI", period="350d", progress=False)
    hsi_series = hsi_raw['Close'].iloc[:, 0] if isinstance(hsi_raw['Close'], pd.DataFrame) else hsi_raw['Close']

    # 2. TV 名册
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {
        "columns": ["name", "description", "close", "market_cap_basic", "VWAP", "volume"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.1e10}],
        "range": [0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    resp = requests.post(url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([
        {"code": re.sub(r'[^0-9]', '', d['d'][0]), 
         "mkt": d['d'][3], 
         "vwap": d['d'][4] or d['d'][2], 
         "turnover": (d['d'][2] * d['d'][5]) if d['d'][5] else 0} for d in resp
    ])

    print(f" -> 🌐 同步腾讯汉化名录...")
    name_map = get_chinese_names(df_pool['code'].tolist())

    # 3. 分批演算
    final_list = []
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code']]
    chunk_size = 40
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 📥 正在处理 {i+1} ~ {min(i+chunk_size, len(tickers))} 支...")
        # 增加重试逻辑
        try:
            data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
            for t in chunk:
                try:
                    stock_df = data[t].dropna()
                    code_clean = t.split('.')[0].lstrip('0')
                    row_info = df_pool[df_pool['code'] == code_clean].iloc[0]
                    res = analyze_v32(t, name_map.get(code_clean, t), stock_df, row_info['mkt'], row_info['turnover'], row_info['vwap'], hsi_series)
                    if res: final_list.append(res)
                except: continue
        except Exception as e:
            print(f" -> ⚠️ 批次下载失败: {e}")
        time.sleep(2)

    if not final_list:
        print("⚠️ 今日无符合标的。")
        return

    # 4. 计算 RS 评级
    res_df = pd.DataFrame(final_list)
    res_df['RS评分'] = res_df['rs_raw'].rank(pct=True).apply(lambda x: int(x*99))
    res_df = res_df.sort_values(by="RS评分", ascending=False).head(50)

    # 5. 精准写入
    sh = init_v32_sheet()
    sh.clear()
    
    output_cols = ["Ticker", "Name", "Price", "POC", "VWAP", "Dist_POC", "60D_Ret", "RSI", "Vol_Ratio", "Mkt_Cap", "Turnover", "Trend", "RS评分"]
    header = [output_cols]
    sh.update(range_name="A1", values=header + res_df[output_cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("M2", now_str) 

    # 6. 视觉美化 (修正 TextFormat 参数)
    set_frozen(sh, rows=1)
    rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range('M2:M100', sh)],
        booleanRule=BooleanRule(
            condition=BooleanCondition('NUMBER_GREATER', ['89']),
            # 👈 修复：foregroundColor 代替 color
            format=cellFormat(
                textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0, 0)), 
                backgroundColor=color(1, 0.9, 0.9)
            )
        )
    )
    set_conditional_format_rules(sh, [rule])

    print(f"✅ V32.2 任务成功！")

if __name__ == "__main__":
    main()
