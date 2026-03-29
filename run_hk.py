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
# 1. 核心配置 (精准锁定 GID)
# ==========================================
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit"
TARGET_GID = 665566258  # 👈 这是你浏览器里看到的那个页面的 ID
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_v28_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    
    print(f" -> 📄 成功访问文档: '{doc.title}'")
    
    # 遍历所有标签页，寻找匹配 GID 的那一页
    worksheets = doc.worksheets()
    for ws in worksheets:
        if ws.id == TARGET_GID:
            print(f" -> 🎯 成功锁定目标标签页: '{ws.title}' (ID: {ws.id})")
            return ws
            
    # 如果找不到，就取第一个作为后备
    print(f" -> ⚠️ 未找到 GID {TARGET_GID}，使用默认第一页")
    return doc.get_worksheet(0)

# ==========================================
# 🧠 2. 策略引擎 (保持严选逻辑)
# ==========================================

def calculate_v28_metrics(df_h, hsi_series):
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
    ma50, ma150, ma200 = np.mean(close[-50:]), np.mean(close[-150:]), np.mean(close[-200:])
    low_52w, high_52w = np.min(low[-250:]), np.max(high[-250:])
    
    # 欧奈尔硬性趋势过滤
    if cp < ma150 or cp < ma200 or ma150 < ma200: return None
    if cp < low_52w * 1.25 or cp < high_52w * 0.80: return None
    
    # ADR 活跃度
    adr = np.mean((high[-10:] - low[-10:]) / close[-10:]) * 100
    if adr < 2.0: return None 

    # RS 计算
    try:
        aligned_hsi = hsi_series.reindex(df_h.index).ffill().values
        rs_line = close / aligned_hsi
        rs_slope_now = (rs_line[-1] - rs_line[-10]) / rs_line[-10]
        rs_slope_prev = (rs_line[-11] - rs_line[-20]) / rs_line[-20]
        rs_accel = rs_slope_now - rs_slope_prev
    except: return None

    is_vdu = vol[-1] < np.mean(vol[-50:]) * 0.5
    label = "🔥 引擎加速" if rs_accel > 0 else "📡 轨道维持"
    if is_vdu: label = "💎 窒息枯竭"

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
# 🚀 3. 主流程
# ==========================================

def main():
    print(f"[{datetime.datetime.now(TZ_SHANGHAI).strftime('%H:%M')}] 🚀 V28.0 启动 (GID 锁定版)...")
    
    # 1. 准备大盘
    try:
        hsi_raw = yf.download("^HSI", period="350d", progress=False)
        hsi_series = hsi_raw['Close'].iloc[:, 0] if isinstance(hsi_raw['Close'], pd.DataFrame) else hsi_raw['Close']
        hsi_cp = float(hsi_series.iloc[-1])
        is_safe = hsi_cp > float(hsi_series.rolling(50).mean().iloc[-1])
        print(f" -> 恒指: {hsi_cp:.2f} ({'进攻' if is_safe else '防御'})")
    except: print(" -> ❌ 恒指获取失败"); return

    # 2. 初始池
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "description", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.1e10},
                          {"left": "close", "operation": "greater", "right": 1.0}],
               "range": [0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    tv_data = requests.post(url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "Others"} for d in tv_data])

    # 3. 计算
    final_list = []
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=False)
    
    for _, row in df_pool.iterrows():
        t = str(row['code']).zfill(4)+".HK"
        try:
            stock_df = data[t].dropna() if len(tickers) > 1 else data.dropna()
            m = calculate_v28_metrics(stock_df, hsi_series)
            if m:
                m.update({"代码": row['code'], "行业": row['sector']})
                final_list.append(m)
        except: continue

    print(f" -> 精选后标的: {len(final_list)} 支")

    # 4. 写入表格
    sh = init_v28_sheet()
    sh.clear()
    
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    
    if not final_list:
        sh.update(range_name="A1", values=[[f"最後更新: {now_str} | 今日無符合標的"]])
        return

    res_df = pd.DataFrame(final_list)
    res_df['RS评级'] = res_df['RS_Raw'].rank(pct=True).apply(lambda x: int(x*99))
    res_df['综合得分'] = (res_df['RS评级'] * 0.7) + (res_df['ADR'] * 2.5)
    
    final_output = res_df.sort_values(by="综合得分", ascending=False).groupby('行业').head(3)
    final_output = final_output[["代码", "RS评级", "Action", "综合得分", "VDU", "ADR", "Ext50", "行业", "StopLoss"]].head(40)

    header = [f"大盘: {'进攻' if is_safe else '防御'}", f"更新: {now_str}", f"选股数: {len(final_output)}", "", "", "", "", "", ""]
    data_to_write = [header] + [final_output.columns.values.tolist()] + final_output.values.tolist()

    sh.update(range_name="A1", values=data_to_write)
    
    # 美化
    set_frozen(sh, rows=2)
    format_cell_range(sh, 'A1:I2', cellFormat(textFormat=textFormat(bold=True), backgroundColor=color(0.9, 0.9, 0.9)))
    print(f"✅ V28.0 推送成功至标签页: '{sh.title}'")

if __name__ == "__main__":
    main()
