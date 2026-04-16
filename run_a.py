import yfinance as yf
import pandas as pd
import numpy as np
import datetime, requests, json, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxq2QHGFGY_-X4FwQO8Ix52t7jq5Fo-uaRvhs32lzco0_NsfNtWZzsnXIDWsRIMpi16/exec"

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

def analyze_stock(df_s, bench_series, t_code):
    try:
        c = df_s['Close'].dropna()
        if isinstance(c, pd.DataFrame): c = c.iloc[:, 0]
        h = df_s['High'].dropna()
        if isinstance(h, pd.DataFrame): h = h.iloc[:, 0]
        l = df_s['Low'].dropna()
        if isinstance(l, pd.DataFrame): l = l.iloc[:, 0]
        v = df_s['Volume'].dropna()
        if isinstance(v, pd.DataFrame): v = v.iloc[:, 0]

        curr, prev = float(c.iloc[-1]), float(c.iloc[-2])
        
        # RS
        s_ret = (curr / float(c.iloc[-min(len(c), 120)])) - 1
        b_ret = (float(bench_series.iloc[-1]) / float(bench_series.iloc[-min(len(bench_series), 120)])) - 1
        rs = float(round((s_ret - b_ret + 1) * 85, 2))

        # MA50
        ma50 = c.rolling(50).mean()
        m50_c = float(ma50.iloc[-1])
        slope = (m50_c - float(ma50.iloc[-6])) / float(ma50.iloc[-6]) * 100 if len(ma50)>6 else 0

        # 量价
        v_r = float(v.iloc[-1] / v.iloc[-21:-1].mean())
        dist = float(((curr / h.tail(22).max()) - 1) * 100)
        amp = float((float(h.iloc[-1]) - float(l.iloc[-1])) / prev * 100)

        level, act, win, guide = "⚪ 观察", "潜伏观察", "40%", "等待回踩"
        near_ma50 = (m50_c * 0.97) <= curr <= (m50_c * 1.03)
        
        if rs > 85:
            if near_ma50 and v_r < 0.7:
                level, act, win, guide = "🚀 进攻", "🐍 毒蛇出洞", "85%", "地量踩线，买入胜率高"
            elif dist > -5:
                level, act, win, guide = "🔥 强势", "禁追 (高位)", "60%", "高位强势，勿盲目追"
            else:
                level, act, win, guide = "🟡 准备", "等待缩量", "55%", "趋势强劲，等回踩缩量"
        elif curr < m50_c * 0.96:
            level, act, win, guide = "💀 破位", "放弃 (跌穿)", "10%", "跌破50日线，风险极大"

        return [t_code, act, level, f"{dist:.1f}%", f"{v_r:.2f}x", f"{amp:.1f}%", 
                "📈 向上" if slope > 0 else "📉 向下", win, guide, rs]
    except: return None

def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"VP-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 V60.10 扫描开始 | ID: {trace_id}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        idx = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        bench = idx['Close']
        if isinstance(bench, pd.DataFrame): bench = bench.iloc[:, 0]
        all_data = yf.download(tickers, period="1y", group_by='ticker', progress=False, auto_adjust=True)
    except Exception as e:
        print(f"❌ 下载失败: {e}"); return

    results = []
    for t_full in tickers:
        try:
            res = analyze_stock(all_data[t_full], bench, t_full.split('.')[0])
            if res: results.append(res); print(f"✅ {t_full.split('.')[0]}")
        except: continue

    results.sort(key=lambda x: (x[2], x[9]), reverse=True)
    
    header = [
        ["🚀 V60.10 毒蛇量化", "ID:", trace_id, "策略:", "MA50地量回踩", "更新:", dt_str, "", "", ""], 
        ["代码", "交易指令", "信号等级", "距高点", "量比", "振幅", "MA50趋势", "预测胜率", "实战指引", "RS分"]
    ]
    
    if results:
        try:
            payload = json.loads(json.dumps(header + results, default=safe_convert))
            resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
            print(f"📡 Google 响应: {resp.text}")
        except Exception as e: print(f"❌ 推送失败: {e}")

if __name__ == "__main__":
    main()
