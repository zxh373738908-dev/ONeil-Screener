import yfinance as yf
import pandas as pd
import numpy as np
import datetime, requests, json, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
# 【重要】请替换为你刚才新部署得到的 URL
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyYfpfYNyRhXcyZrfIHEyErECMM82xkCKfZm71RUZ1YL6Xjr5Kca3ruoVJzxcNAwH9q/exec"

CORE_TICKERS_RAW = [
    "300502", "300308", "300394", "688313", "002428", "003031", "600519", "300750",
    "601138", "688041", "601899", "601857", "000951", "000333", "603259", "603501",
    "002371", "002475", "002594", "600030", "002222", "603799", "601208", "600105"
]

def format_ticker(code):
    c = str(code).zfill(6)
    return f"{c}.SS" if c.startswith('6') else f"{c}.SZ"

def safe_convert(obj):
    if isinstance(obj, (np.integer, np.floating)): 
        return float(obj) if not np.isnan(obj) else 0.0
    return str(obj)

# ==========================================
# 2. V60.15 狙击引擎 (逻辑继承自 V14)
# ==========================================
def analyze_stock_v15(df_s, bench_series, t_code):
    try:
        c = df_s['Close'].ffill().dropna()
        if isinstance(c, pd.DataFrame): c = c.iloc[:, 0]
        h = df_s['High'].ffill().dropna()
        l = df_s['Low'].ffill().dropna()
        v = df_s['Volume'].ffill().dropna()

        if len(c) < 60: return None
        curr, prev = float(c.iloc[-1]), float(c.iloc[-2])
        
        # 1. RS 评分
        def get_safe_ret(ser, d):
            return (ser.iloc[-1] / ser.iloc[-min(len(ser), d)]) - 1
        rs = round((get_safe_ret(c, 120)*0.4 + get_safe_ret(c, 20)*0.6 - 
                    get_safe_ret(bench_series, 20)*1.0 + 1) * 85, 2)
        
        # 2. 均线
        ma20 = c.rolling(20).mean().iloc[-1]
        ma50 = c.rolling(50).mean().iloc[-1]
        
        # 3. 核心抓手：静默收缩
        v_r = float(v.iloc[-1] / v.iloc[-21:-1].mean())
        amp = (float(h.iloc[-1]) - float(l.iloc[-1])) / prev * 100 
        dist_h = ((curr / h.tail(10).max()) - 1) * 100 
        
        level, act, win, guide = "⚪ 观察", "潜伏观察", "45%", "无明显信号"
        
        # 判定：谁是起跳黑马
        is_squeeze = amp < 2.5 and v_r < 0.65
        is_strong_trend = curr > ma20 and ma20 > ma50
        
        if rs > 90 and is_strong_trend:
            if is_squeeze and -3 < dist_h <= 0.1:
                level, act, win, guide = "🚀 进攻", "静默狙击", "88%", "极窄波动+地量,随时爆发"
            elif ma20 * 0.985 <= curr <= ma20 * 1.015:
                level, act, win, guide = "🎯 进攻", "生命线支撑", "82%", "20日线精准支撑"
            else:
                level, act, win, guide = "🟡 准备", "强势整理", "65%", "趋势向上,等横盘"
        elif rs > 75 and ma50 * 0.98 <= curr <= ma50 * 1.02 and v_r < 0.6:
            level, act, win, guide = "🐍 潜伏", "毒蛇出洞", "75%", "50日线地量"

        return [t_code, act, level, f"{dist_h:.2f}%", f"{v_r:.2f}x", f"{amp:.2f}%", 
                "多头排列" if (ma20 > ma50) else "震荡", f"{win}", guide, f"{rs:.2f}"]
    except: return None

# ==========================================
# 3. 主程序
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"V15-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 V60.15 运行 | 目标: A-Share screener | ID: {trace_id}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        data = yf.download(tickers, period="1y", group_by='ticker', progress=False, auto_adjust=True)
        idx = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        bench = idx['Close'].ffill().iloc[:,0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close'].ffill()
    except Exception as e:
        print(f"❌ 失败: {e}"); return

    results = []
    for t_full in tickers:
        t_raw = t_full.split('.')[0]
        try:
            res = analyze_stock_v15(data[t_full], bench, t_raw)
            if res: results.append(res); print(f"✅ {t_raw}")
        except: continue

    results.sort(key=lambda x: (float(x[7].replace('%','')), float(x[9])), reverse=True)
    
    header = [
        ["🚀 V60.15 黎明狙击", "ID:", trace_id, "策略:", "定向同步 A-Share screener", "更新:", dt_str, "", "", ""], 
        ["代码", "交易指令", "信号等级", "距高点", "量比", "振幅", "趋势背景", "胜率预测", "实战指引", "RS分"]
    ]
    
    try:
        payload = json.loads(json.dumps(header + results, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"📡 Google 响应: {resp.text}")
    except: print(f"❌ 推送失败")

if __name__ == "__main__":
    main()
