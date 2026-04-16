import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests, time
import yfinance as yf

# ================= 配置区 =================
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

# 策略参数
RSI_PERIOD = 14
RSI_THRESHOLD = 30      # RSI 低于 30 (超跌)
BIAS_THRESHOLD = -10.0  # 乖离率低于 -10% (严重偏离均线)
# ==========================================

def init_sheet():
    """初始化并获取 Google Sheet 工作表"""
    creds = Credentials.from_service_account_file(
        CREDS_FILE, 
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try:
        return doc.worksheet(TARGET_SHEET_NAME)
    except:
        return doc.add_worksheet(TARGET_SHEET_NAME, 1000, 20)

def calculate_rsi(series, period=14):
    """手动计算 RSI 指标"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def get_stock_pool():
    """从 TradingView 扫描市值 > 80亿的前 500 只股票"""
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "industry", "close", "market_cap_basic"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 500] # 扫描范围扩大到 500 以增加触发概率
    }
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json()
        raw_data = resp.get('data', [])
        pool = []
        for item in raw_data:
            s_code = item['s']
            raw_symbol = s_code.split(':')[-1]
            # 转换为 yfinance 格式
            yf_code = f"{raw_symbol}.SS" if raw_symbol.startswith('6') else f"{raw_symbol}.SZ"
            pool.append({
                "yf_code": yf_code,
                "symbol": raw_symbol,
                "name": item['d'][1], # description
                "industry": item['d'][2],
                "price": item['d'][3]
            })
        return pool
    except Exception as e:
        print(f"❌ 获取候选池失败: {e}")
        return []

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 开始 V53.3 泣血早鸟深度扫描...")
    
    pool = get_stock_pool()
    if not pool: return
    print(f"✅ 成功锁定 {len(pool)} 只大市值候选标的，开始指标计算...")

    results = []
    
    # 遍历下载并分析 (yf.download 建议小批量或单只增加容错)
    for i, stock in enumerate(pool):
        try:
            # 下载最近 60 天数据
            df = yf.download(stock['yf_code'], period="60d", interval="1d", progress=False)
            
            if df.empty or len(df) < 20:
                continue
            
            # 处理 yfinance 可能返回的多重索引列
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # --- 技术指标计算 ---
            close_prices = df['Close']
            curr_price = float(close_prices.iloc[-1])
            
            # 1. 计算 RSI
            rsi_series = calculate_rsi(close_prices, RSI_PERIOD)
            curr_rsi = rsi_series.iloc[-1]
            
            # 2. 计算 MA20 乖离率 (BIAS)
            ma20 = close_prices.rolling(window=20).mean().iloc[-1]
            bias = ((curr_price - ma20) / ma20) * 100
            
            # 3. 计算 5 日成交量均值
            vol_ma5 = df['Volume'].rolling(window=5).mean().iloc[-1]
            curr_vol = df['Volume'].iloc[-1]

            # --- 泣血早鸟触发逻辑 ---
            # 条件：RSI 超跌 + 严重负乖离
            is_blood_bird = curr_rsi < RSI_THRESHOLD and bias < BIAS_THRESHOLD
            
            if is_blood_bird:
                print(f"⭐ 命中: {stock['name']} ({stock['symbol']}) | RSI: {curr_rsi:.2f} | BIAS: {bias:.2f}%")
                results.append({
                    "代码": stock['symbol'],
                    "名称": stock['name'],
                    "现价": round(curr_price, 2),
                    "RSI(14)": round(curr_rsi, 2),
                    "MA20乖离率": f"{round(bias, 2)}%",
                    "行业": stock['industry'],
                    "更新时间": now.strftime('%H:%M:%S')
                })
            
            # 每 10 只打印一次进度
            if i % 50 == 0:
                print(f"   进度: {i}/{len(pool)}...")

        except Exception as e:
            # print(f"⚠️ 跳过 {stock['yf_code']}: {e}")
            continue

    # --- 结果写入 Google Sheets ---
    sh = init_sheet()
    
    if not results:
        print("⚠️ 扫描完毕：未发现符合“泣血早鸟”极端超跌形态的标的。")
        sh.update_acell("N1", f"最后扫描(无结果): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        # 整理数据
        df_final = pd.DataFrame(results)
        
        # 清除旧数据并写入新数据
        sh.clear()
        # 写入表头和内容
        sh.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        
        # 更新时间戳到 N1 单元格
        sh.update_acell("N1", f"最后成功扫描: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎊 扫描结束！共发现 {len(results)} 只标的，已同步至 Google Sheets。")

if __name__ == "__main__":
    run_v53_optimizer()
