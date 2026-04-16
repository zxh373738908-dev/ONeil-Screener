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

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 开始执行 V53.3 泣血早鸟深度扫描...")
    
    # 1. 优化后的数据抓取逻辑 (增加容错)
    try:
        idx_s = yf.download("000300.SS", period="400d", progress=False)['Close']
        idx_s = idx_s.iloc[:, 0] if isinstance(idx_s, pd.DataFrame) else idx_s
        
        tv_url = "https://scanner.tradingview.com/china/scan"
        payload = {
            "columns": ["name", "description", "industry", "close", "market_cap_basic"],
            "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}],
            "range": [0, 200]
        }
        resp = requests.post(tv_url, json=payload, timeout=15).json()
        raw_data = resp.get('data', [])
        
        pool = []
        for item in raw_data:
            # 鲁棒性解析：确保 'd' 列表长度足够
            d = item.get('d', [])
            if len(d) >= 4:
                pool.append({
                    "code": item['s'], # 使用 symbol 字段，更加稳健
                    "name": d[0], 
                    "industry": d[2], 
                    "price": d[3]
                })
        print(f"✅ 获取到 {len(pool)} 只标的候选...")
    except Exception as e:
        print(f"❌ 数据源解析失败: {e}"); return

    # 2. 核心扫描 (其余逻辑不变)
    results = []
    # ... (后续循环扫描逻辑保持不变)
    
    # 3. 如果结果为空，提示并更新时间戳
    if not results:
        print("⚠️ 未发现符合泣血早鸟条件的标的。")
        sh = init_sheet()
        sh.update_acell("N1", f"最后扫描(无结果): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        # 写入结果逻辑
        pass

if __name__ == "__main__":
    run_v53_optimizer()
