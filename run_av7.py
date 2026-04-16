import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests, os
import yfinance as yf

# 配置
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try: return doc.worksheet(TARGET_SHEET_NAME)
    except: return doc.add_worksheet(TARGET_SHEET_NAME, 1000, 20)

def get_engine_data(df, idx_df):
    try:
        # 基础计算
        c = df['Close'].astype(float); h = df['High'].astype(float); l = df['Low'].astype(float); v = df['Volume'].astype(float)
        price = c.iloc[-1]; ma50 = c.rolling(50).mean().iloc[-1]
        
        # 1. 核心过滤：盈亏比与紧致度
        tightness = (h.tail(10).max() - l.tail(10).min()) / (l.tail(10).min() + 0.001) * 100
        tr = pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        
        stop = round(price - atr * 1.5, 2)
        target = round(h.tail(250).max(), 2)
        rrr = round((target - price) / (price - stop + 0.001), 1)
        
        # 2. 只有满足条件的票才返回
        if rrr < 2.0 or price < ma50 * 0.95: return None
        
        # 3. RS 计算
        rs_line = c / idx_df
        rs_raw = (rs_line.iloc[-1] / rs_line.tail(250).min()) # 相对强度因子
        
        return {
            "RS评级": int(rs_raw * 10), "量比": round(v.iloc[-1]/(v.rolling(20).mean().iloc[-1]+0.01), 2),
            "紧致度": round(tightness, 2), "50日乖离": round((price/ma50-1)*100, 2),
            "盈亏比": rrr, "现价": price, "止损": stop, "目标": target,
            "RS线新高": "✅" if rs_line.iloc[-1] >= rs_line.tail(250).max()*0.98 else "❌"
        }
    except: return None

def run_scanner():
    # 1. 获取基准与池子
    idx_s = yf.download("000300.SS", period="400d", progress=False)['Close'].iloc[:, 0]
    # ... (省略网络获取逻辑，同前文) ...
    
    # 2. 处理 (模拟逻辑)
    # 扫描代码... 
    # 结果装入 final_data
    
    # 3. 输出表格
    sh = init_sheet()
    header = ["代码", "名称", "行业", "RS评级", "量比", "紧致度", "50日乖离", "盈亏比", "现价", "止损", "目标", "RS线新高"]
    # ... 更新逻辑 ...
    print("✅ 扫描完成，已按盈亏比优化排序。")

if __name__ == "__main__":
    run_scanner()
