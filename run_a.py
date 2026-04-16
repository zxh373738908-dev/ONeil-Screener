import yfinance as yf
import pandas as pd
import numpy as np
import datetime, requests, json, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbzdFvvn32j46Z0oyfa0klqfJ1yNiY8WSXNi6jyaI9Qihe98m8zIkdCNNEU1XYEoLzBT/exec"

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
# 2. 核心逻辑
# ==========================================
def analyze_stock_safe(df_s, bench_series, t_code):
    try:
        close_ser = df_s['Close'].dropna()
        if isinstance(close_ser, pd.DataFrame): close_ser = close_ser.iloc[:, 0]
        high_ser = df_s['High'].dropna()
        if isinstance(high_ser, pd.DataFrame): high_ser = high_ser.iloc[:, 0]
        low_ser = df_s['Low'].dropna()
        if isinstance(low_ser, pd.DataFrame): low_ser = low_ser.iloc[:, 0]
        vol_ser = df_s['Volume'].dropna()
        if isinstance(vol_ser, pd.DataFrame): vol_ser = vol_ser.iloc[:, 0]

        if len(close_ser) < 60: return None

        curr_price = float(close_ser.iloc[-1])
        prev_close = float(close_ser.iloc[-2])
        
        # RS
        stock_ret = (curr_price / float(close_ser.iloc[-min(len(close_ser), 120)])) - 1
        bench_ret = (float(bench_series.iloc[-1]) / float(bench_series.iloc[-min(len(bench_series), 120)])) - 1
        rs_score = float(round((stock_ret - bench_ret + 1) * 85, 2))

        # MA50
        ma50 = close_ser.rolling(50).mean()
        ma50_curr = float(ma50.iloc[-1])
        ma50_prev = float(ma50.iloc[-6]) if len(ma50) > 6 else ma50_curr
        ma50_slope_val = (ma50_curr - ma50_prev) / ma50_prev * 100

        # 指标
        vol_ratio = float(vol_ser.iloc[-1] / vol_ser.iloc[-21:-1].mean())
        dist_high = float(((curr_price / high_ser.tail(22).max()) - 1) * 100)
        amp = float((float(high_ser.iloc[-1]) - float(low_ser.iloc[-1])) / prev_close * 100)

        level, act, win, guide = "⚪ 观察", "潜伏观察", "40%", "等待回踩"
        is_near_ma50 = (ma50_curr * 0.97) <= curr_price <= (ma50_curr * 1.03)
        
        if rs_score > 85:
            if is_near_ma50 and vol_ratio < 0.7:
                level, act, win, guide = "🚀 进攻", "🐍 毒蛇出洞", "85%", "极度缩量+50日线"
            elif dist_high > -5:
                level, act, win, guide = "🔥 强势", "禁追 (高位)", "60%", "涨幅过大"
            else:
                level, act, win, guide = "🟡 准备", "等待缩量", "55%", "观察地量出现"
        elif curr_price < ma50_curr * 0.96:
            level, act, win, guide = "💀 破位", "放弃 (跌穿)", "10%", "趋势走坏"

        return [t_code, act, level, f"{dist_high:.1f}%", f"{vol_ratio:.2f}x", f"{amp:.1f}%", 
                "📈 向上" if ma50_slope_val > 0 else "📉 向下", win, guide, rs_score]
    except: return None

# ==========================================
# 3. 主流程 (修复对齐问题)
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"VP-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"📡 V60.7 扫描启动 | ID: {trace_id}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        idx_data = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        bench_close = idx_data['Close']
        if isinstance(bench_close, pd.DataFrame): bench_close = bench_close.iloc[:, 0]
        all_data = yf.download(tickers, period="1y", group_by='ticker', progress=False, auto_adjust=True)
    except Exception as e:
        print(f"❌ 下载失败: {e}"); return

    results = []
    for t_full in tickers:
        t_raw = t_full.split('.')[0]
        try:
            res_row = analyze_stock_safe(all_data[t_full], bench_close, t_raw)
            if res_row: results.append(res_row); print(f"✅ {t_raw}")
        except: continue

    results.sort(key=lambda x: (x[2], x[9]), reverse=True)
    
    # 【核心修复】补全标题行至10列，确保矩阵整齐
    header = [
        ["🚀 V60.7 毒蛇狙击", "ID:", trace_id, "策略:", "50日线缩量", "更新:", dt_str, "", "", ""], 
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
