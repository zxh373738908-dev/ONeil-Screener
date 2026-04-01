import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime
import time
import math

# ==========================================
# 1. 核心配置
# ==========================================
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_SHEET_NAME = "us Screener" 
CREDS_FILE = "credentials.json"

# 缩减到 15 只最核心股票，确保 100% 下载成功
CORE_TICKERS = ["NVDA", "TSLA", "PLTR", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "AMD", "CF", "PR", "MSTR", "U", "COIN", "MARA"]

def sync_to_sheets_ultra_stable(matrix):
    """极简写入逻辑，专门对抗 char 1 错误"""
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID).worksheet(TARGET_SHEET_NAME)
        
        # 使用最原始的 update 方式
        sh.clear()
        time.sleep(1) # 强制停顿，等 Google 响应
        sh.update('A1', matrix)
        return True
    except Exception as e:
        print(f"❌ Google Sheets 写入失败: {e}")
        return False

# ==========================================
# 2. 执行引擎
# ==========================================
def run_debug_scan():
    print(f"🚀 系统启动...")
    start_time = time.time()

    # 1. 下载 SPY (大盘参考)
    print("📥 正在获取大盘数据...")
    spy = yf.Ticker("SPY").history(period="100d")
    if spy.empty:
        print("❌ 无法获取 SPY 数据，检查网络")
        return
    spy_close = spy['Close'].iloc[-1]
    
    # 2. 逐个下载股票 (虽然慢一点点，但比批量下载更稳，不容易被封)
    candidates = []
    print(f"🔍 扫描核心池 (共 {len(CORE_TICKERS)} 只)...")
    
    for t in CORE_TICKERS:
        try:
            ticker_obj = yf.Ticker(t)
            df = ticker_obj.history(period="100d")
            if df.empty or len(df) < 50:
                continue
            
            # 极简逻辑：只要 5 日均线在 20 日均线上方就抓取 (确保一定有结果)
            ma5 = df['Close'].rolling(5).mean().iloc[-1]
            ma20 = df['Close'].rolling(20).mean().iloc[-1]
            curr = df['Close'].iloc[-1]
            
            if ma5 > ma20:
                candidates.append([
                    t, 
                    "🔥多头" if curr > ma5 else "✅观察", 
                    f"{round(curr, 2)}",
                    f"{round(((curr/df['Close'].iloc[-20])-1)*100, 2)}%"
                ])
                print(f"  + 发现信号: {t}")
        except:
            continue

    # 3. 构造数据矩阵
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    
    header = [
        ["🏰 V1000 调试版", "Time:", bj_now, "SPY:", round(spy_close, 2)],
        ["代码", "状态", "现价", "20日涨幅"]
    ]
    
    if not candidates:
        matrix = header + [["⚠️ 暂无满足多头逻辑的股票"]]
    else:
        matrix = header + candidates

    # 4. 写入
    print("📤 正在同步至 Google Sheets...")
    success = sync_to_sheets_ultra_stable(matrix)
    
    total_time = time.time() - start_time
    if success:
        print(f"✅ 运行成功！耗时: {round(total_time, 2)}秒")
    else:
        print(f"❌ 运行失败，请检查 Credentials.json 是否有效或 API 是否开启")

if __name__ == "__main__":
    run_debug_scan()
