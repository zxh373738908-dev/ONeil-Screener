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
# 1. 核心配置 (用 KEY 锁定，拒绝重名文档)
# ==========================================
# 从你的 URL 中提取的唯一 ID
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_GID = 665566258  
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_v29_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    
    # 使用 open_by_key 确保绝不写错文件
    doc = client.open_by_key(SS_KEY)
    
    print(f" -> 🎯 已精准锁定唯一文档: '{doc.title}' (ID: {SS_KEY})")
    
    worksheets = doc.worksheets()
    for ws in worksheets:
        if ws.id == TARGET_GID:
            print(f" -> ✅ 已锁定目标标签页: '{ws.title}' (GID: {ws.id})")
            return ws
    return doc.get_worksheet(0)

# ==========================================
# 🧠 2. 策略引擎 (根据你的截图，恢复了你最喜欢的列结构)
# ==========================================

def calculate_v29_metrics(df_h, hsi_series):
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
    ma20 = np.mean(close[-20:])
    ma50 = np.mean(close[-50:])
    ma200 = np.mean(close[-200:])
    
    # 基础趋势过滤
    if cp < ma200: return None 

    # ADR 活跃度
    adr = np.mean((high[-10:] - low[-10:]) / close[-10:]) * 100
    if adr < 2.0: return None 

    # RS 演算 (这里模拟你的 60D_Return 和 RSI)
    ret_60d = (cp / close[-60] - 1) * 100
    
    # 模拟一个 RSI (14)
    delta = np.diff(close[-20:])
    gain = np.mean(delta[delta > 0]) if any(delta > 0) else 0
    loss = -np.mean(delta[delta < 0]) if any(delta < 0) else 1
    rsi = 100 - (100 / (1 + gain/loss))

    # 操作标记 (老龙回头逻辑)
    avg_vol50 = np.mean(vol[-50:])
    vol_ratio = vol[-1] / avg_vol50
    
    is_dragon = (cp > ma200) and (abs(cp-ma50)/ma50 < 0.03) and (vol[-1] < avg_vol50 * 0.7)
    
    action = "📈 经典多头"
    if is_dragon: action = "🐉 老龙回头"
    elif vol_ratio > 2 and cp > close[-2]: action = "🚀 放量起爆"

    return {
        "Ticker": "", # 占位
        "Name": "",   # 占位
        "Price": round(float(cp), 2),
        "60D_Ret(%)": round(ret_60d, 2),
        "RSI": round(rsi, 2),
        "Vol_Ratio": round(vol_ratio, 2),
        "Action": action,
        "StopLoss": round(cp * 0.93, 2), # 简单 7% 止损
        "Raw_RS": (cp / close[-250]) / (hsi_series.iloc[-1] / hsi_series.iloc[-250])
    }

# ==========================================
# 🚀 3. 主流程
# ==========================================

def main():
    print(f"[{datetime.datetime.now(TZ_SHANGHAI).strftime('%H:%M')}] 🚀 V29.0 指挥官系统启动...")
    
    # 1. 准备大盘
    hsi_raw = yf.download("^HSI", period="350d", progress=False)
    hsi_series = hsi_raw['Close'].iloc[:, 0] if isinstance(hsi_raw['Close'], pd.DataFrame) else hsi_raw['Close']
    
    # 2. 获取池子
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "description", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.1e10},
                          {"left": "close", "operation": "greater", "right": 1.0}],
               "range": [0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    tv_data = requests.post(url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "name": d['d'][1], "sector": d['d'][4]} for d in tv_data])

    # 3. 批量演算
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=False)
    
    final_list = []
    for _, row in df_pool.iterrows():
        t = str(row['code']).zfill(4)+".HK"
        try:
            stock_df = data[t].dropna()
            m = calculate_v29_metrics(stock_df, hsi_series)
            if m:
                m.update({"Ticker": row['code'], "Name": row['name']})
                final_list.append(m)
        except: continue

    print(f" -> 精选后标的: {len(final_list)} 支")

    # 4. 写入表格
    sh = init_v29_sheet()
    
    # 写入一个显著的更新标记到 Z1
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    sh.update_acell("Z1", f"FORCE_REFRESH: {now_str}")

    if not final_list:
        sh.update_acell("A1", f"今日無符合標的 - {now_str}")
        return

    # 处理结果
    res_df = pd.DataFrame(final_list)
    res_df['RS评级'] = res_df['Raw_RS'].rank(pct=True).apply(lambda x: int(x*99))
    res_df = res_df.sort_values(by="RS评级", ascending=False).head(40)
    
    # 整理列顺序 (尝试贴近你的截图风格)
    output = res_df[["Ticker", "Name", "Price", "60D_Ret(%)", "RSI", "Vol_Ratio", "Action", "RS评级", "StopLoss"]]
    
    # 执行写入
    sh.clear()
    header = [["Ticker", "Name", "Price", "60D_Ret(%)", "RSI", "Vol_Ratio", "Trend_Action", "RS_Rank", "Stop_Loss", "", "Updated_At:", now_str]]
    sh.update(range_name="A1", values=header + output.values.tolist())
    
    # 格式化
    set_frozen(sh, rows=1)
    format_cell_range(sh, 'A1:L1', cellFormat(textFormat=textFormat(bold=True), backgroundColor=color(0.9, 0.9, 0.9)))
    
    print(f"✅ V29.0 推送成功。请刷新 GID {TARGET_GID} 页面查看！")

if __name__ == "__main__":
    main()
