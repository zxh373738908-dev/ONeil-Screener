import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import math
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (精简了无效标的)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyVyClySDklAPrx30URWZOGyb423Vb_5Dzt7WCKQ6WJwcNb7HLqwD0ckiMYm5sTwnLz/exec"

# 删除了失效的 WBT，增加了当前最热门的动量标度
CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL"
]

SECTOR_MAP = {
    "NVDA": "AI/半导体", "AMD": "AI/半导体", "AVGO": "AI/半导体", "SMCI": "AI/半导体", "SOXL": "半导体杠杆",
    "TSLA": "新能源", "PLTR": "软件/AI", "MSFT": "软件/AI", "GOOGL": "软件/AI",
    "MSTR": "加密货币", "COIN": "加密货币", "MARA": "加密货币", "CLSK": "加密货币", "BITF": "加密货币",
    "AAPL": "消费电子", "META": "社交/AI", "AMZN": "电商/云", 
    "VRT": "基础设施", "ANET": "基础设施", "HOOD": "金融/Crypto", "LLY": "生物医药"
}

# ==========================================
# 2. 核心算法 (修正了 EMA 逻辑与 RS 周期)
# ==========================================
def calculate_v1000_reactor(ticker, spy_df):
    try:
        # 改为逐个下载，确保数据纯净
        df = yf.download(ticker, period="1y", interval="1d", progress=False)
        if df.empty or len(df) < 50: return None
        
        # 强制展平 MultiIndex（防止 yfinance 新版返回多级索引）
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        close = df['Close']
        high = df['High']
        low = df['Low']
        curr_price = float(close.iloc[-1])
        
        # 1. ADR 过滤 (20日)
        adr = float(((high - low) / low).tail(20).mean() * 100)
        if adr < 2.5: return None # 稍微放宽到2.5，捕捉更多强势股

        # 2. Trend_Align (主升浪排列)
        ema10 = close.ewm(span=10, adjust=False).mean().iloc[-1]
        ma20 = close.rolling(window=20).mean().iloc[-1]
        sma50 = close.rolling(window=50).mean().iloc[-1]
        is_aligned = (curr_price > ema10 > ma20 > sma50)
        trend_status = "✅多头" if is_aligned else "❌破位"

        # 3. RS 强度评分 (对比 SPY)
        # 确保日期对齐
        spy_aligned = spy_df.reindex(df.index).ffill()
        def get_rel_perf(d):
            if len(close) < d: return 0
            stock_ret = (curr_price - close.iloc[-d]) / close.iloc[-d]
            spy_ret = (float(spy_aligned.iloc[-1]) - float(spy_aligned.iloc[-d])) / float(spy_aligned.iloc[-d])
            return stock_ret - spy_ret

        # 综合 RS (近3个月权重最大)
        rs_score = (get_rel_perf(63) * 3 + get_rel_perf(126) * 2 + get_rel_perf(250)) * 10
        
        # 4. 紧致度 (VCP倾向)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)

        # 信号定义
        signals = []
        base_res = 0
        if is_aligned: signals.append("🌊主升"); base_res += 3
        if rs_score > 1.0: signals.append("🚀强RS"); base_res += 2
        if curr_price >= float(high.tail(20).max()) * 0.98: signals.append("🎯临界"); base_res += 1

        return {
            "Ticker": ticker, "Price": curr_price, "ADR": adr, "Trend": trend_status,
            "RS": rs_score, "Tight": tightness, "Res": base_res, "Signals": signals
        }
    except Exception as e:
        print(f"Error {ticker}: {e}")
        return None

# ==========================================
# 3. 执行引擎
# ==========================================
def run_v1000_safe_update():
    start_time = time.time()
    print(f"🔥 V1000 10.1 修正版启动 | 时间: {datetime.datetime.now().strftime('%H:%M:%S')}")

    # 先下载大盘基准
    spy_df = yf.download("SPY", period="1y", progress=False)['Close']
    vix_df = yf.download("^VIX", period="1d", progress=False)['Close']
    current_vix = float(vix_df.iloc[-1]) if not vix_df.empty else 0.0

    results = []
    # 逐一处理，防止数据污染
    for t in CORE_TICKERS:
        res = calculate_v1000_reactor(t, spy_df)
        if res:
            results.append(res)
            print(f"✅ {t} 处理完成 | 价格: {res['Price']:.2f} | ADR: {res['ADR']:.2f}%")
        time.sleep(0.1) # 避免请求过快

    if not results:
        print("❌ 未抓取到有效数据"); return

    # 排序
    sorted_data = sorted(results, key=lambda x: (x['Res'], x['RS']), reverse=True)
    
    # 格式化输出
    final_list = []
    sector_counts = {}
    for r in sorted_data:
        sector = SECTOR_MAP.get(r['Ticker'], "其他")
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    for r in sorted_data:
        sector = SECTOR_MAP.get(r['Ticker'], "其他")
        final_list.append([
            r['Ticker'],
            "💎SSS 共振" if r['Res'] >= 5 else ("🔥活跃" if r['Res'] >= 3 else "⚙️观察"),
            " + ".join(r['Signals']) if r['Signals'] else "---",
            f"{sector_counts.get(sector)}只活跃",
            r['Trend'],
            round(r['Price'], 2),
            f"{round(r['ADR'], 2)}%",
            round(r['RS'], 2),
            f"{round(r['Tight'], 2)}%",
            sector
        ])

    # 构造表头
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🌋 V1000 10.1 修正版", "Update:", bj_now, "VIX:", round(current_vix, 2), "状态: 强制刷新", "", "", "", ""],
        ["代码", "评级", "核心信号", "板块集群", "趋势对齐", "现价", "ADR(爆点)", "RS强度", "紧致度", "板块"]
    ]
    
    payload = header + final_list
    
    try:
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 更新成功！耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v1000_safe_update()
