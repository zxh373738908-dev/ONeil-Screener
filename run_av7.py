import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests
import yfinance as yf

# ================= 配置区 =================
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

# 策略阈值（如果觉得结果太少，可以微调这里）
RSI_STRICT = 30
BIAS_STRICT = -8.0  # 修改为-8%，对大盘股更友好
# ==========================================

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try: return doc.worksheet(TARGET_SHEET_NAME)
    except: return doc.add_worksheet(TARGET_SHEET_NAME, 1000, 20)

def get_stock_pool():
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "industry", "close", "market_cap_basic"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 500]
    }
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json()
        return resp.get('data', [])
    except: return []

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 开始 V53.3 增强版扫描...")
    
    raw_data = get_stock_pool()
    if not raw_data: return

    # 1. 预处理代码列表
    stock_map = {}
    tickers = []
    for item in raw_data:
        code = item['s'].split(':')[-1]
        yf_code = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        tickers.append(yf_code)
        stock_map[yf_code] = {"name": item['d'][1], "industry": item['d'][2], "symbol": code}

    # 2. 批量下载 (分块以防请求过大)
    print(f"📥 正在获取 {len(tickers)} 只标的的 K 线数据...")
    data = yf.download(tickers, period="60d", interval="1d", group_by='ticker', progress=True, threads=True)

    results = []
    candidates_debug = [] # 用于记录最接近条件的股票

    # 3. 核心计算
    for yf_code in tickers:
        try:
            # 提取单只股票的 Close
            if yf_code not in data or data[yf_code].empty: continue
            df = data[yf_code].dropna(subset=['Close'])
            if len(df) < 20: continue

            close = df['Close']
            curr_price = float(close.iloc[-1])
            
            # RSI 计算
            delta = close.diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rsi = 100 - (100 / (1 + (gain / loss))).iloc[-1]

            # BIAS 20 计算
            ma20 = close.rolling(20).mean().iloc[-1]
            bias = ((curr_price - ma20) / ma20) * 100

            # 调试记录：记录全市场最超跌的前5名
            candidates_debug.append({"name": stock_map[yf_code]['name'], "rsi": rsi, "bias": bias})

            # 判断逻辑
            if rsi < RSI_STRICT and bias < BIAS_STRICT:
                results.append({
                    "代码": stock_map[yf_code]['symbol'],
                    "名称": stock_map[yf_code]['name'],
                    "现价": round(curr_price, 2),
                    "RSI": round(rsi, 2),
                    "乖离率": f"{round(bias, 2)}%",
                    "行业": stock_map[yf_code]['industry'],
                    "状态": "🔥 泣血早鸟信号"
                })
        except: continue

    # 4. 排序并展示调试信息
    candidates_debug = sorted(candidates_debug, key=lambda x: x['rsi'])[:5]
    print("\n💡 当前市场最接近超跌的标的:")
    for c in candidates_debug:
        print(f"   - {c['name']}: RSI={c['rsi']:.1f}, 乖离={c['bias']:.1f}%")

    # 5. 写入 Google Sheets
    sh = init_sheet()
    sh.clear()
    
    if not results:
        msg = f"最后扫描: {now.strftime('%H:%M:%S')} (未发现极端超跌信号)"
        sh.update_acell("A1", msg)
        print(f"⚠️ {msg}")
    else:
        df_res = pd.DataFrame(results)
        sh.update([df_res.columns.values.tolist()] + df_res.values.tolist())
        sh.update_acell("N1", f"发现时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"✅ 成功发现 {len(results)} 只标的！")

if __name__ == "__main__":
    run_v53_optimizer()
