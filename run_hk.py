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

warnings.filterwarnings('ignore')

# ==========================================
# 1. 核心配置
# ==========================================
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit#gid=0"
SHEET_NAME = "HK-Share Screener" # 👈 确保这里的名字和你的 Google Sheet 标签名完全一致
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_v27_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    try:
        return doc.worksheet(SHEET_NAME) # 👈 按名字寻找标签页
    except:
        return doc.get_worksheet(0) # 找不到就用第一个

# ==========================================
# 🧠 2. V27.3 欧奈尔核心引擎
# ==========================================

def calculate_v27_metrics(df_h, hsi_series):
    if df_h.empty or len(df_h) < 250: return None
    
    try:
        if isinstance(df_h['Close'], pd.DataFrame):
            close = df_h['Close'].iloc[:, 0].values
            high = df_h['High'].iloc[:, 0].values
            low = df_h['Low'].iloc[:, 0].values
            vol = df_h['Volume'].iloc[:, 0].values
        else:
            close = df_h['Close'].values
            high = df_h['High'].values
            low = df_h['Low'].values
            vol = df_h['Volume'].values
    except: return None

    cp = close[-1]
    
    # --- A. 欧奈尔/米纳尔维尼 趋势模板过滤 ---
    ma50 = np.mean(close[-50:])
    ma150 = np.mean(close[-150:])
    ma200 = np.mean(close[-200:])
    low_52w = np.min(low[-250:])
    high_52w = np.max(high[-250:])
    
    # 铁律 1: 股价必须在 150天和200天线线上方
    if cp < ma150 or cp < ma200: return None
    # 铁律 2: 150天线在200天线上方 (长线走牛)
    if ma150 < ma200: return None
    # 铁律 3: 股价距离52周低点至少涨了25%
    if cp < low_52w * 1.25: return None
    # 铁律 4: 股价距离52周高点在25%以内 (高位强势)
    if cp < high_52w * 0.75: return None

    # B. ADR 活跃度 (恢复到 2.2% 确保质量)
    adr = np.mean((high[-10:] - low[-10:]) / close[-10:]) * 100
    if adr < 2.2: return None 

    # C. RS 演算
    try:
        aligned_hsi = hsi_series.reindex(df_h.index).ffill().values
        rs_line = close / aligned_hsi
        rs_slope_now = (rs_line[-1] - rs_line[-10]) / rs_line[-10]
        rs_slope_prev = (rs_line[-11] - rs_line[-20]) / rs_line[-20]
        rs_accel = rs_slope_now - rs_slope_prev
    except: return None

    # D. 状态标记
    is_vdu = vol[-1] < np.mean(vol[-50:]) * 0.5
    
    label = "📡 轨道维持"
    if is_vdu and cp > ma50: label = "💎 窒息枯竭"
    elif rs_accel > 0 and rs_slope_now > 0: label = "🔥 引擎加速"

    # ATR 止损
    tr = np.maximum(high - low, np.maximum(abs(high - np.roll(close, 1)), abs(low - np.roll(close, 1))))
    atr = pd.Series(tr).rolling(20).mean().iloc[-1]

    return {
        "Price": round(float(cp), 2),
        "ADR": round(float(adr), 2),
        "RS_Raw": rs_line[-1] * 100,
        "RS_Accel": round(float(rs_accel * 100), 4),
        "VDU": "YES" if is_vdu else "",
        "Action": label,
        "Ext50": round(float((cp/ma50-1)*100), 1),
        "StopLoss": round(float(cp - (atr * 2.1)), 2)
    }

# ==========================================
# 🚀 3. 执行流程
# ==========================================

def main():
    print(f"[{datetime.datetime.now(TZ_SHANGHAI).strftime('%H:%M')}] 🚀 V27.3 欧奈尔严选版启动...")
    
    try:
        hsi_raw = yf.download("^HSI", period="350d", progress=False)
        hsi_series = hsi_raw['Close'].iloc[:, 0] if isinstance(hsi_raw['Close'], pd.DataFrame) else hsi_raw['Close']
        hsi_cp = float(hsi_series.iloc[-1])
        is_safe = hsi_cp > float(hsi_series.rolling(50).mean().iloc[-1])
        print(f" -> 恒指: {hsi_cp:.2f} ({'进攻' if is_safe else '防御'})")
    except Exception as e:
        print(f" -> ❌ 恒指获取失败: {e}"); return

    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "description", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.2e10}, # 提高到120亿
                          {"left": "close", "operation": "greater", "right": 1.0}],
               "range": [0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        tv_data = requests.post(url, json=payload, timeout=15).json().get('data', [])
        print(f" -> 初始池: {len(tv_data)} 支")
    except: print(" -> ❌ TV抓取失败"); return

    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "Others"} for d in tv_data])

    final_list = []
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=False)
    
    for _, row in df_pool.iterrows():
        t = str(row['code']).zfill(4)+".HK"
        try:
            stock_df = data[t].dropna() if len(tickers) > 1 else data.dropna()
            m = calculate_v27_metrics(stock_df, hsi_series)
            if m:
                m.update({"代码": row['code'], "行业": row['sector']})
                final_list.append(m)
        except: continue

    print(f" -> 精选后标的: {len(final_list)} 支")

    # 更新 Sheets
    sh = init_v27_sheet()
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    sh.clear()
    
    if not final_list:
        sh.update([ [f"最後更新: {now_str} | 今日無符合強勢趨勢標的"] ], "A1")
        return

    res_df = pd.DataFrame(final_list)
    # RS评级 (百分位)
    res_df['RS评级'] = res_df['RS_Raw'].rank(pct=True).apply(lambda x: int(x*99))
    sector_alpha = res_df.groupby('行业')['RS评级'].mean().to_dict()
    res_df['行业Alpha'] = res_df['行业'].map(sector_alpha).round(1)
    res_df['综合得分'] = (res_df['RS评级'] * 0.6) + (res_df['行业Alpha'] * 0.2) + (res_df['ADR'] * 2)
    
    # 最终取精华
    final_output = res_df.sort_values(by="综合得分", ascending=False).groupby('行业').head(3)
    final_output = final_output[["代码", "RS评级", "Action", "综合得分", "VDU", "ADR", "Ext50", "行业", "StopLoss"]].head(35)

    header = [f"大盘: {'进攻' if is_safe else '防御'}", f"更新: {now_str}", f"选股总数: {len(final_output)}", "", "", "", "", "", ""]
    sh.update([header] + [final_output.columns.values.tolist()] + final_output.values.tolist(), "A1")
    
    set_frozen(sh, rows=2)
    format_cell_range(sh, 'A1:I1', cellFormat(textFormat=textFormat(bold=True), backgroundColor=color(0.95, 0.95, 0.95)))
    format_cell_range(sh, 'A2:I2', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0.2, 0.2, 0.2)))
    
    print(f"✅ V27.3 推送成功。")

if __name__ == "__main__":
    main()
