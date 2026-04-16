import yfinance as yf
import pandas as pd
import numpy as np
import datetime, requests, json, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
# 【重要】请替换为你最新部署得到的 URL
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbytCLB6vbSBDZpwPQX6FAhSNyCFlsnWHChOX6vs89WWVYKAorvnAS8jEx9WXNoP79Ef/exec"

CORE_TICKERS_RAW = [
    "600519", "300750", "601138", "300502", "603501", "688041", "002371", "300308",
    "002475", "002594", "601899", "600030", "600900", "600150", "300274", "000333",
    "688981", "300763", "002415", "603259", "601318", "000651", "600585", "000725",
    "000951", "601857", "600019", "000895"
]

def format_ticker(code):
    c = str(code).zfill(6)
    return f"{c}.SS" if c.startswith('6') else f"{c}.SZ"

def safe_convert(obj):
    if isinstance(obj, (np.integer, np.floating)): 
        return float(obj) if not np.isnan(obj) else 0.0
    return str(obj)

# ==========================================
# 2. 分析引擎
# ==========================================
def analyze_stock_safe(df_s, bench_series, t_code):
    try:
        # 提取序列并转为单列 Series 防止歧义
        c = df_s['Close'].dropna()
        if isinstance(c, pd.DataFrame): c = c.iloc[:, 0]
        h = df_s['High'].dropna()
        if isinstance(h, pd.DataFrame): h = h.iloc[:, 0]
        l = df_s['Low'].dropna()
        if isinstance(l, pd.DataFrame): l = l.iloc[:, 0]
        v = df_s['Volume'].dropna()
        if isinstance(v, pd.DataFrame): v = v.iloc[:, 0]

        if len(c) < 60: return None
        curr, prev = float(c.iloc[-1]), float(c.iloc[-2])
        
        # 1. RS 评分
        s_ret = (curr / float(c.iloc[-min(len(c), 120)])) - 1
        b_ret = (float(bench_series.iloc[-1]) / float(bench_series.iloc[-min(len(bench_series), 120)])) - 1
        rs = float(round((s_ret - b_ret + 1) * 85, 2))

        # 2. MA50 与斜率
        ma50 = c.rolling(50).mean()
        m50_c = float(ma50.iloc[-1])
        slope = (m50_c - float(ma50.iloc[-6])) / float(ma50.iloc[-6]) * 100 if len(ma50)>6 else 0

        # 3. 核心量价
        v_r = float(v.iloc[-1] / v.iloc[-21:-1].mean())
        dist = float(((curr / h.tail(22).max()) - 1) * 100)
        amp = float((float(h.iloc[-1]) - float(l.iloc[-1])) / prev * 100)

        # 4. 判定逻辑
        level, act, win, guide = "⚪ 观察", "潜伏观察", "40%", "等待回踩"
        near_ma50 = (m50_c * 0.97) <= curr <= (m50_c * 1.03)
        
        if rs > 85:
            if near_ma50 and v_r < 0.7:
                level, act, win, guide = "🚀 进攻", "🐍 毒蛇出洞", "85%", "极度缩量踩线，买入胜率高"
            elif dist > -5:
                level, act, win, guide = "🔥 强势", "禁追 (高位)", "60%", "高位横盘中，勿追"
            else:
                level, act, win, guide = "🟡 准备", "等待缩量", "55%", "趋势极强，等缩量回踩"
        elif curr < m50_c * 0.96:
            level, act, win, guide = "💀 破位", "放弃 (跌穿)", "10%", "跌破生命线，已走弱"

        return [t_code, act, level, f"{dist:.1f}%", f"{v_r:.2f}x", f"{amp:.1f}%", 
                "📈 向上" if slope > 0 else "📉 向下", win, guide, rs]
    except: return None

# ==========================================
# 3. 主程序
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"VIPER-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 V60.9 终极扫描启动 | ID: {trace_id}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        idx = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        bench = idx['Close']
        if isinstance(bench, pd.DataFrame): bench = bench.iloc[:, 0]
        all_data = yf.download(tickers, period="1y", group_by='ticker', progress=False, auto_adjust=True)
    except Exception as e:
        print(f"❌ 数据下载失败: {e}"); return

    results = []
    for t_full in tickers:
        t_raw = t_full.split('.')[0]
        try:
            res = analyze_stock_safe(all_data[t_full], bench, t_raw)
            if res: results.append(res); print(f"✅ {t_raw} OK")
        except: continue

    results.sort(key=lambda x: (x[2], x[9]), reverse=True)
    
    header = [
        ["🚀 V60.9 毒蛇量化 (自动化版)", "ID:", trace_id, "模式:", "MA50地量回踩", "更新:", dt_str, "", "", ""], 
        ["代码", "交易指令", "信号等级", "距高点", "量比", "振幅", "MA50趋势", "预测胜率", "实战指引", "RS分"]
    ]
    
    if results:
        try:
            payload = json.loads(json.dumps(header + results, default=safe_convert))
            print(f"📡 正在推送到 Google Sheets...")
            resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
            print(f"📡 Google 响应: {resp.text}")
        except Exception as e: print(f"❌ 推送失败: {e}")

if __name__ == "__main__":
    main()
