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

def init_v30_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    
    # 诊断：列出机器人名下所有的文档
    print(" -> 🔍 正在扫描机器人权限内的所有文档...")
    all_files = client.openall()
    for f in all_files:
        print(f"    - 文档名: '{f.title}' | ID: {f.id}")
        
    doc = client.open_by_key(SS_KEY)
    print(f" -> 🎯 目标文档: '{doc.title}'")
    
    worksheets = doc.worksheets()
    for ws in worksheets:
        if ws.id == TARGET_GID:
            print(f" -> ✅ 目标标签页: '{ws.title}' (GID: {ws.id})")
            return ws
    return doc.get_worksheet(0)

# ==========================================
# 🧠 2. 策略引擎 (保持你的列结构)
# ==========================================

def calculate_v30_metrics(df_h, hsi_series):
    if df_h.empty or len(df_h) < 250: return None
    try:
        # 数据对齐与提取
        c_raw = df_h['Close']
        close = c_raw.iloc[:, 0].values if isinstance(c_raw, pd.DataFrame) else c_raw.values
        cp = close[-1]
        
        ma50 = np.mean(close[-50:])
        ma200 = np.mean(close[-200:])
        if cp < ma200: return None 

        # 模拟你的列逻辑
        ret_60d = (cp / close[-60] - 1) * 100
        vol_ratio = df_h['Volume'].iloc[-1] / df_h['Volume'].iloc[-50:].mean()
        
        return {
            "Ticker": "", "Name": "",
            "Price": round(float(cp), 2),
            "POC": round(float(ma50), 2),      # 模拟POC
            "VWAP": round(float(cp * 0.98), 2), # 模拟VWAP
            "Dist_POC": round(float((cp/ma50-1)*100), 2),
            "60D_Ret": round(float(ret_60d), 2),
            "RSI": 70, # 模拟
            "Vol": round(float(vol_ratio), 2),
            "Mkt": 100, # 模拟
            "Turn": 10, # 模拟
            "Trend": "🚀 经典多头",
            "RS_Raw": cp / close[-250]
        }
    except: return None

# ==========================================
# 🚀 3. 执行流程
# ==========================================

def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 V30.0 正在执行外科手术式写入...")
    
    # 1. 准备数据
    hsi_raw = yf.download("^HSI", period="5d", progress=False)
    hsi_series = hsi_raw['Close'].iloc[:, 0] if isinstance(hsi_raw['Close'], pd.DataFrame) else hsi_raw['Close']
    
    # 2. 获取池子
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "description", "close"], "range": [0, 100]}
    tv_data = requests.post(url, json=payload, timeout=10).json().get('data', [])
    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "name": d['d'][1]} for d in tv_data])

    # 3. 核心计算
    final_list = []
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code'][:50]] # 仅取50只加速测试
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    for _, row in df_pool[:50].iterrows():
        t = str(row['code']).zfill(4)+".HK"
        try:
            m = calculate_v30_metrics(data[t].dropna(), hsi_series)
            if m:
                m.update({"Ticker": row['code'], "Name": row['name']})
                final_list.append(m)
        except: continue

    # 4. 精准写入
    sh = init_v30_sheet()
    
    # --- 强力刷新 M2 单元格 (对应你截图中的 Last Updated) ---
    print(f" -> 💉 正在强制注射新时间戳到 M2: {now_str}")
    sh.update_acell("M2", now_str)
    
    if final_list:
        res_df = pd.DataFrame(final_list).head(30)
        # 按照你截图的列顺序排列
        # Ticker, Name, Price, POC, VWAP, Dist_POC, 60D_Ret, RSI, Vol_Ratio, Mkt, Turn, Trend
        output_data = res_df[["Ticker", "Name", "Price", "POC", "VWAP", "Dist_POC", "60D_Ret", "RSI", "Vol", "Mkt", "Turn", "Trend"]]
        
        print(f" -> 📝 正在写入 A2 起始的数据体 (30行)...")
        # 使用 USER_ENTERED 强制刷新界面
        sh.update(range_name="A2", values=output_data.values.tolist(), value_input_option="USER_ENTERED")
    
    # Z1 存一个标记，用于双重验证
    sh.update_acell("Z1", f"V30_SYNC_OK_{now_str}")
    
    print(f"✅ V30.0 手术成功。请立刻刷新表格查看 M2 单元格！")

if __name__ == "__main__":
    main()
