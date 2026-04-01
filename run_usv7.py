import yfinance as yf
import pandas as pd
import datetime
import requests
import json
import time
import numpy as np

# ==========================================
# 1. 核心配置 (已绑定你的专属 URL)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyVyClySDklAPrx30URWZOGyb423Vb_5Dzt7WCKQ6WJwcNb7HLqwD0ckiMYm5sTwnLz/exec"

# 扫描池：包含你关注的重点标的
CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "CF", "PR", "GOOGL", "AAPL", "MSFT", "META", 
    "AMZN", "AMD", "MSTR", "COIN", "MARA", "SMCI", "AVGO", "LLY"
]

def clean_for_json(val):
    """确保所有数据都能被 Google 识别，处理 NaN 和 Numpy 类型"""
    if pd.isna(val) or val is None: return ""
    if isinstance(val, (np.floating, float)): return float(round(val, 2))
    if isinstance(val, (np.integer, int)): return int(val)
    return str(val)

# ==========================================
# 2. 闪电执行引擎
# ==========================================
def run_v1000_bridge():
    print(f"🚀 [V1000] 桥接系统启动...")
    start_time = time.time()

    # 1. 批量获取数据 (约 5-8 秒)
    print("📥 正在同步市场数据...")
    try:
        # 下载 100 天数据以计算均线
        data = yf.download(CORE_TICKERS, period="100d", group_by='ticker', threads=True, progress=False)
        spy = yf.download("SPY", period="100d", progress=False)['Close']
    except Exception as e:
        print(f"❌ 下载失败: {e}"); return

    candidates = []
    print(f"🔍 正在执行枢纽算法...")

    for t in CORE_TICKERS:
        try:
            df = data[t].dropna()
            if len(df) < 50: continue
            
            close = df['Close']
            curr = close.iloc[-1]
            ma20 = close.rolling(20).mean().iloc[-1]
            ma50 = close.rolling(50).mean().iloc[-1]
            
            # 简单有效的枢纽逻辑：价格在均线上方 + 近期走势强于大盘
            # 计算 20 日涨幅
            perf_20d = (curr / close.iloc[-20] - 1) * 100
            
            # 信号判定
            signal = ""
            if curr > ma20 and curr > ma50:
                if curr > close.tail(60).max() * 0.98:
                    signal = "🚀巅峰突破"
                elif close.tail(10).std() / close.tail(10).mean() < 0.015:
                    signal = "👁️奇点先行"
                else:
                    signal = "✅多头趋势"

            if signal:
                candidates.append([
                    t, 
                    signal, 
                    clean_for_json(curr), 
                    f"{clean_for_json(perf_20d)}%",
                    "强于大盘" if perf_20d > 5 else "跟随"
                ])
        except: continue

    # 2. 构造发送矩阵
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    
    # 表头设计
    header = [
        ["🏰 V1000 终极枢纽 (桥接版)", "", "最后更新(北京):", bj_now, ""],
        ["-"*10, "-"*10, "-"*10, "-"*10, "-"*10],
        ["代码 (Ticker)", "扫描信号", "现价 (Price)", "20日表现", "相对强度"]
    ]
    
    # 合并数据
    matrix = header + candidates if candidates else header + [["📭 暂时没有符合信号的股票"]]

    # 3. 发送至 Google Apps Script (约 2-3 秒)
    print(f"📤 正在通过加密隧道传输至 Google Sheets...")
    try:
        # 设置超时，防止网络卡死
        response = requests.post(WEBAPP_URL, data=json.dumps(matrix), timeout=15)
        
        if response.text == "Success":
            print(f"🎉 同步达成！数据已更新至 'us Screener' 标签页。")
        else:
            print(f"⚠️ 响应异常: {response.text}")
    except Exception as e:
        print(f"❌ 传输失败，请检查网络: {e}")

    print(f"🏁 任务完成。总耗时: {round(time.time() - start_time, 2)} 秒")

if __name__ == "__main__":
    run_v1000_bridge()
