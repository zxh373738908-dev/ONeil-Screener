import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 
# ==========================================
WEBAPP_URL = "您的_GOOGLE_APPS_SCRIPT_WEBAPP_URL" # 请替换为您真实的URL

# 核心标的池
CORE_TICKERS =[
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
# 2. 核心算法 (引入 Minervini 趋势模板与量价分析)
# ==========================================
def calculate_high_probability_reactor(ticker, spy_df):
    try:
        # 获取 1 年以上数据以计算 200日均线和 52周高低点
        df = yf.download(ticker, period="2y", interval="1d", progress=False)
        if df.empty or len(df) < 250: 
            return None
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        close = df['Close']
        high = df['High']
        low = df['Low']
        vol = df['Volume']
        
        curr_price = float(close.iloc[-1])
        curr_vol = float(vol.iloc[-1])
        
        # --- 指标计算 ---
        sma50 = close.rolling(window=50).mean().iloc[-1]
        sma150 = close.rolling(window=150).mean().iloc[-1]
        sma200 = close.rolling(window=200).mean().iloc[-1]
        vol_sma50 = vol.rolling(window=50).mean().iloc[-1]
        
        high_52w = float(high.tail(250).max())
        low_52w = float(low.tail(250).min())
        
        # 1. ADR 过滤 (20日平均真实波幅)
        adr = float(((high - low) / low).tail(20).mean() * 100)

        # 2. 严格的 Minervini 第二阶段 (Stage 2) 趋势模板验证 (提升胜率的核心)
        cond_1 = curr_price > sma150 and curr_price > sma200
        cond_2 = sma150 > sma200
        cond_3 = curr_price > sma50
        cond_4 = curr_price >= low_52w * 1.25  # 距离52周低点至少反弹25%
        cond_5 = curr_price >= high_52w * 0.75 # 距离52周高点在25%以内
        
        is_stage_2 = cond_1 and cond_2 and cond_3 and cond_4 and cond_5
        
        if is_stage_2:
            trend_status = "🏆S2主升浪"
        elif curr_price > sma50 and curr_price > sma200:
            trend_status = "✅多头震荡"
        else:
            trend_status = "❌破位/弱势"

        # 3. RS 强度评分与大盘对比 (更平滑的算法)
        spy_aligned = spy_df.reindex(df.index).ffill()
        def get_rel_perf(d):
            if len(close) < d: return 0
            stock_ret = (curr_price - close.iloc[-d]) / close.iloc[-d]
            spy_ret = (float(spy_aligned.iloc[-1]) - float(spy_aligned.iloc[-d])) / float(spy_aligned.iloc[-d])
            return stock_ret - spy_ret

        # 强化近3个月和6个月的权重，捕捉最强动能
        rs_score = (get_rel_perf(63) * 4 + get_rel_perf(126) * 2 + get_rel_perf(250)) * 10

        # 4. 量价形态 (VPA) 与 紧致度
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # 量价逻辑判定
        is_vol_contracting = vol.tail(3).mean() < vol_sma50 * 0.75 # 极致缩量
        is_vol_expanding = curr_vol > vol_sma50 * 1.5 # 显著放量
        is_up_day = curr_price > close.iloc[-2]
        is_near_high = curr_price >= high_52w * 0.95 # 距离新高不到5%

        # 5. 信号定义与严格打分系统
        signals =[]
        base_res = 0
        
        if is_stage_2: base_res += 3
        if rs_score > 2.0: signals.append("🚀极强RS"); base_res += 2
        
        # 高胜率形态判定
        if is_near_high and is_up_day and is_vol_expanding:
            signals.append("💥巨量突破"); base_res += 4
        elif is_near_high and tightness < 3.5 and is_vol_contracting:
            signals.append("🤐VCP临界"); base_res += 3 # 胜率最高的潜伏点
        elif is_near_high:
            signals.append("🎯前高阻力"); base_res += 1

        # 若不在第二阶段且没有量，降级处理 (避免买入垃圾股)
        if not is_stage_2 and base_res > 0:
            base_res -= 2 

        return {
            "Ticker": ticker, "Price": curr_price, "ADR": adr, "Trend": trend_status,
            "RS": rs_score, "Tight": tightness, "Res": base_res, "Signals": signals
        }
    except Exception as e:
        print(f"Error {ticker}: {e}")
        return None

# ==========================================
# 3. 执行引擎与大盘红绿灯
# ==========================================
def run_v1000_institutional_update():
    start_time = time.time()
    print(f"🔥 V1000 12.0 机构高胜率版启动 | 时间: {datetime.datetime.now().strftime('%H:%M:%S')}")

    # 下载大盘基准
    spy_df = yf.download("SPY", period="2y", progress=False)['Close']
    if isinstance(spy_df, pd.DataFrame): spy_df = spy_df.iloc[:, 0]
    
    vix_df = yf.download("^VIX", period="1d", progress=False)['Close']
    current_vix = float(vix_df.iloc[-1]) if not vix_df.empty else 0.0

    # 宏观大盘红绿灯 (Market Regime)
    spy_curr = spy_df.iloc[-1]
    spy_ma20 = spy_df.rolling(20).mean().iloc[-1]
    spy_ma50 = spy_df.rolling(50).mean().iloc[-1]
    
    if spy_curr > spy_ma20 and spy_curr > spy_ma50:
        market_env = "🟢 安全 (重仓突破)"
    elif spy_curr > spy_ma50:
        market_env = "🟡 震荡 (降低仓位)"
    else:
        market_env = "🔴 危险 (严禁追高)"

    results =[]
    for t in CORE_TICKERS:
        res = calculate_high_probability_reactor(t, spy_df)
        if res:
            results.append(res)
            print(f"✅ {t} 完成 | 现价: {res['Price']:.2f} | 评分: {res['Res']} | 趋势: {res['Trend']}")
        time.sleep(0.1)

    if not results:
        print("❌ 未抓取到有效数据"); return

    # 排序：总得分优先，同分看相对强度和波动率(ADR)
    sorted_data = sorted(results, key=lambda x: (x['Res'], x['RS'], x['ADR']), reverse=True)
    
    final_list =[]
    sector_counts = {}
    for r in sorted_data:
        sector = SECTOR_MAP.get(r['Ticker'], "其他")
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    for r in sorted_data:
        sector = SECTOR_MAP.get(r['Ticker'], "其他")
        
        # 评级系统调整，满分由严格条件决定
        if r['Res'] >= 7: rating = "💎满仓共振"
        elif r['Res'] >= 4: rating = "🔥高胜率买点"
        elif r['Res'] >= 2: rating = "👀右侧关注"
        else: rating = "⚙️垃圾时间"

        final_list.append([
            r['Ticker'],
            rating,
            " + ".join(r['Signals']) if r['Signals'] else "---",
            f"{sector_counts.get(sector)}只活跃",
            r['Trend'],
            round(r['Price'], 2),
            f"{round(r['ADR'], 2)}%",
            round(r['RS'], 2),
            f"{round(r['Tight'], 2)}%",
            sector
        ])

    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    
    # 构造表头，将大盘红绿灯加入
    header = [["🌋 V1000 12.0 机构版", "Update:", bj_now, "大盘环境:", market_env, "VIX:", round(current_vix, 2), "模型状态: 开启量价过滤", "", ""],["代码", "交易评级", "高胜率信号", "板块集群", "长线趋势", "现价", "ADR(爆点)", "RS强度", "紧致度", "板块"]
    ]
    
    payload = header + final_list
    
    try:
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 同步至表格成功！耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v1000_institutional_update()
