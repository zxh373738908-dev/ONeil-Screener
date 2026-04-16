import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests, os
import yfinance as yf

# 环境屏蔽
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# 配置
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
    
    # 1. 获取基准与股票池
    try:
        idx_s = yf.download("000300.SS", period="400d", progress=False)['Close']
        idx_s = idx_s.iloc[:, 0] if isinstance(idx_s, pd.DataFrame) else idx_s
        
        tv_url = "https://scanner.tradingview.com/china/scan"
        payload = {"columns":["name","industry","close"], "filter":[{"left":"market_cap_basic","operation":"greater","right":80e8}], "range":[0, 200]}
        resp = requests.post(tv_url, json=payload, timeout=10).json().get('data', [])
        pool = [{"code": d['d'][0], "name": d['d'][1], "industry": d['d'][2], "price": d['d'][3]} for d in resp]
    except Exception as e:
        print(f"❌ 初始化失败: {e}"); return

    results = []
    # 2. 核心扫描
    for item in pool:
        try:
            ticker = f"{item['code']}.SS" if item['code'].startswith('6') else f"{item['code']}.SZ"
            df = yf.download(ticker, period="1y", progress=False)
            if len(df) < 200: continue
            
            c = df['Close']; h = df['High']; l = df['Low']; v = df['Volume']
            price = c.iloc[-1]
            
            # 基础指标
            ma50 = c.rolling(50).mean().iloc[-1]
            atr = (h-l).rolling(14).mean().iloc[-1]
            tightness = (h.tail(10).max() - l.tail(10).min()) / (l.tail(10).min() + 0.001) * 100
            
            # 过滤规则
            if price < ma50 * 0.95 or tightness > 10: continue
            
            # 数据封装
            rs_line = c / idx_s
            rrr = round((h.tail(250).max() - price) / (atr * 1.5 + 0.01), 1)
            
            if rrr > 2.0: # 泣血早鸟阈值
                results.append({
                    "代码": item['code'], "名称": item['name'], "行业": item['industry'],
                    "RS评级": int((rs_line.iloc[-1]/rs_line.tail(250).min()) * 10),
                    "量比": round(v.iloc[-1]/(v.rolling(20).mean().iloc[-1]+0.1), 2),
                    "紧致度": round(tightness, 2), "50日乖离": round((price/ma50-1)*100, 2),
                    "盈亏比": rrr, "现价": price, "止损": round(price - atr*1.5, 2),
                    "目标": round(h.tail(250).max(), 2),
                    "RS线新高": "✅" if rs_line.iloc[-1] >= rs_line.tail(250).max()*0.98 else "❌"
                })
        except: continue

    # 3. 更新表格
    if results:
        df_out = pd.DataFrame(results).sort_values("盈亏比", ascending=False)
        sh = init_sheet()
        sh.clear() # 强制清空
        # 写入表头及数据
        sh.update(range_name="A1", values=[df_out.columns.tolist()] + df_out.values.tolist(), value_input_option="USER_ENTERED")
        # 写入时间戳
        sh.update_acell("N1", f"最后扫描: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎉 任务完成！已成功抓取 {len(results)} 只标的。")
    else:
        print("⚠️ 未发现符合泣血早鸟标准的标的。")

if __name__ == "__main__":
    run_v53_optimizer()
