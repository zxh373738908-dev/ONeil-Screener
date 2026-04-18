import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import requests
import warnings
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
# 替换为你的 Google Script 部署 URL
WEBAPP_URL = "https://script.google.com/macros/s/你的ID/exec"

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL", "ARM", "MU", "TSM"
]

# ==========================================
# 2. 核心逻辑插件
# ==========================================
def get_perf(series, days):
    """计算指定周期的涨跌幅"""
    if len(series) < days + 1: return 0.0
    return ((series.iloc[-1] / series.iloc[-(days+1)]) - 1) * 100

def process_ticker(symbol, spy_data):
    """单标的深度分析引擎"""
    try:
        tk = yf.Ticker(symbol)
        # 获取 1 年的历史数据 (足以覆盖 60D 和 200D 指标)
        df = tk.history(period="1y")
        if df.empty or len(df) < 65: return None
        
        info = tk.info
        close = df['Close']
        high = df['High']
        low = df['Low']
        vol = df['Volume']
        
        # --- 1. 技术指标计算 ---
        curr_price = close.iloc[-1]
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ema10 = close.ewm(span=10).mean()
        
        # ADR (20日平均波幅)
        adr = ((high - low) / low).tail(20).mean() * 100
        # 量比 (今日成交量 / 20日均量)
        vol_ratio = vol.iloc[-1] / vol.tail(20).mean()
        # 乖离率 (偏离20日线幅度)
        bias = ((curr_price - ma20.iloc[-1]) / ma20.iloc[-1]) * 100
        
        # --- 2. 相对强度与涨幅 ---
        p5d = get_perf(close, 5)
        p20d = get_perf(close, 20)
        p60d = get_perf(close, 60)
        
        spy_20p = get_perf(spy_data, 20)
        spy_60p = get_perf(spy_data, 60)
        r20 = p20d - spy_20p
        r60 = p60d - spy_60p

        # --- 3. V13 多因子评分系统 ---
        score = 0
        # 趋势得分 (S2结构: 价格 > 10 > 20 > 50)
        is_s2 = curr_price > ema10.iloc[-1] > ma20.iloc[-1] > ma50.iloc[-1]
        if is_s2: score += 3
        elif curr_price > ma20.iloc[-1]: score += 1
        
        # 强度得分
        if r20 > 0: score += 1
        if r60 > 0: score += 1
        # 量价确认
        if vol_ratio > 1.2 and close.iloc[-1] > close.iloc[-2]: score += 1
        
        # --- 4. 动作与共振判定 ---
        action = "WAIT"
        if score >= 5: action = "🚀STRONG BUY"
        elif score >= 3: action = "⚖️HOLD/ADD"
        elif curr_price < ma20.iloc[-1]: action = "⚠️REDUCE"
        
        resonance = "No"
        if is_s2 and vol_ratio > 1.1 and r20 > 5:
            resonance = "🔥TRIPLE"

        # --- 5. 格式化输出 ---
        return [
            symbol,
            info.get('industry', 'Index/ETF'),
            score,
            action,
            resonance,
            round(adr, 2),
            round(vol_ratio, 2),
            round(bias, 2),
            f"{info.get('marketCap', 0)/1e9:.1f}B",
            round(score * 16.6, 1), # RS_Rank 模拟
            "Yes" if info.get('optionsExpirationDates') else "No",
            round(curr_price, 2),
            f"{p5d:.2f}%",
            f"{p20d:.2f}%",
            f"{p60d:.2f}%",
            round(r20, 2),
            round(r60, 2)
        ]
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None

# ==========================================
# 3. 执行主引擎
# ==========================================
def run_v13_terminal():
    print(f"🏰 V13 机构终端启动 | {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    # 获取大盘基准
    spy = yf.download("SPY", period="1y", progress=False)['Close']
    
    # 多线程加速获取数据
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_ticker, t, spy) for t in CORE_TICKERS]
        for f in futures:
            res = f.result()
            if res:
                print(f"✅ {res[0]} 分析完成")
                results.append(res)

    # 排序：评分(Score) > 相对强度(R20)
    results.sort(key=lambda x: (x[2], x[15]), reverse=True)

    # 构造矩阵标题
    header = [
        "Ticker", "Industry", "Score", "Action", "Resonance", 
        "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", 
        "Options", "Price", "5D", "20D", "60D", "R20", "R60"
    ]
    
    final_matrix = [header] + results

    # 发送云端或打印预览
    try:
        response = requests.post(WEBAPP_URL, json=final_matrix, timeout=15)
        print(f"🎉 同步成功: {response.status_code}")
    except:
        print("⚠️ 云端同步跳过 (未配置URL)")
        # 打印前5行示例
        for row in final_matrix[:6]:
            print(row)

if __name__ == "__main__":
    run_v13_terminal()
