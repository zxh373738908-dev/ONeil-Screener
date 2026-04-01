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
CORE_TICKERS = ["NVDA", "TSLA", "PLTR", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "AMD", "CF", "PR", "MSTR", "U", "COIN", "MARA"]

# ==========================================
# 2. 增强型同步引擎 (解决 Char 1 报错)
# ==========================================
def sync_to_sheets_final_solution(matrix):
    print("📤 准备上传数据...")
    # 强制在写之前停顿 3 秒，让 Google API 冷却
    time.sleep(3)
    
    try:
        creds = Credentials.from_service_account_file(
            CREDS_FILE, 
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID).worksheet(TARGET_SHEET_NAME)
        
        # --- 策略调整 ---
        # 不使用 clear()，因为 clear 会多产生一次 API 请求，容易触发 Char 1
        # 我们直接用一个巨大的空矩阵“覆盖”旧数据，合并为一个请求
        padding = [["" for _ in range(10)] for _ in range(30)] # 准备 30 行空数据
        # 把实际数据放入空矩阵顶部
        for i, row in enumerate(matrix):
            for j, val in enumerate(row):
                padding[i][j] = str(val)

        # 只发送一次 update 请求
        sh.update('A1:J30', padding)
        return True
        
    except Exception as e:
        error_msg = str(e)
        if "char 1" in error_msg:
            print("⚠️ 仍然触发 Google 防火墙，正在执行强制物理退避...")
            time.sleep(10) # 遇到 char 1 必须死等
            # 最后一次尝试，直接写入，不覆盖
            try:
                sh.update('A1', matrix)
                return True
            except:
                return False
        print(f"❌ 错误详情: {e}")
        return False

# ==========================================
# 3. 主程序
# ==========================================
def run():
    start_time = time.time()
    print(f"🚀 系统启动...")

    # 快速抓取
    candidates = []
    for t in CORE_TICKERS:
        try:
            df = yf.Ticker(t).history(period="20d")
            if not df.empty and df['Close'].iloc[-1] > df['Close'].rolling(5).mean().iloc[-1]:
                candidates.append([t, "✅多头", round(df['Close'].iloc[-1], 2)])
                if len(candidates) >= 10: break # 只抓前10个，确保速度
        except: continue

    # 构造矩阵
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [["🏰 V1000 最终版", "Time:", bj_now], ["代码", "状态", "价格"]]
    matrix = header + candidates

    # 写入
    if sync_to_sheets_final_solution(matrix):
        print(f"🎉 成功！总耗时: {round(time.time() - start_time, 2)}秒")
    else:
        print("❌ 最终尝试失败。建议：1. 检查 API 是否启用；2. 换个网络环境或代理。")

if __name__ == "__main__":
    run()
