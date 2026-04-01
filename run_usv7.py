import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json

# [填入你的 URL]
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyVyClySDklAPrx30URWZOGyb423Vb_5Dzt7WCKQ6WJwcNb7HLqwD0ckiMYm5sTwnLz/exec"

CORE_TICKERS = ["NVDA", "TSLA", "PLTR", "MSTR", "CF", "PR", "GOOGL", "AAPL", "MSFT", "META", "AMZN", "AMD"]

def run_diagnostic_v1000():
    print("🚀 V1000 诊断模式启动...")
    start_time = time.time()

    # 1. 下载数据 (缩短到 6个月加快速度)
    data = yf.download(CORE_TICKERS, period="6mo", group_by='ticker', threads=True, progress=False)
    spy = yf.download("SPY", period="6mo", progress=False)['Close']
    
    results = []
    print(f"🔍 正在扫描 {len(CORE_TICKERS)} 只标的...")
    
    for t in CORE_TICKERS:
        try:
            df = data[t].dropna()
            if len(df) < 50: continue
            curr = df['Close'].iloc[-1]
            ma20 = df['Close'].rolling(20).mean().iloc[-1]
            
            # 放宽逻辑：只要在 20日均线上方就抓取
            if curr > ma20:
                # 每一行固定 6 列
                results.append([
                    t, 
                    "✅多头", 
                    f"{round(curr, 2)}", 
                    f"{round(((curr/df['Close'].iloc[-20])-1)*100, 2)}%",
                    f"{round(df['Volume'].iloc[-1]/df['Volume'].rolling(20).mean().iloc[-1], 2)}x",
                    "监控中"
                ])
        except: continue

    # 2. 构造发送矩阵 (确保每一行都是 6 列)
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🏰 V1000 系统", "诊断模式", "更新:", bj_now, "", ""], # 6列
        ["代码", "状态", "价格", "20日表现", "量比", "备注"]      # 6列
    ]
    
    if not results:
        matrix = header + [["-", "待机", "全市场无多头信号", "-", "-", "-"]]
    else:
        matrix = header + results

    # 打印到控制台看看
    print(f"📊 准备发送 {len(matrix)} 行数据...")
    for r in matrix[:3]: print(f"   样例行: {r}")

    # 3. 发送
    try:
        # 增加 headers 确保传输稳定
        response = requests.post(
            WEBAPP_URL, 
            data=json.dumps(matrix), 
            headers={'Content-Type': 'application/json'},
            timeout=15
        )
        print(f"📡 服务器响应: {response.text}")
        if "Success" in response.text:
            print(f"🎉 同步成功！耗时: {round(time.time() - start_time, 2)}s")
        else:
            print(f"❌ 同步失败，原因见上方的服务器响应")
    except Exception as e:
        print(f"❌ 网络异常: {e}")

if __name__ == "__main__":
    run_diagnostic_v1000()
