import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请换成你刚才“新建部署”得到的 URL)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxcwtfGFZqWyulM2x63ytoYnuYzR-siWVCahjsIqdRbsuYjBac8YCuy7GTRlwd-YGmc/exec"

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
    if isinstance(obj, (np.integer, np.floating)): return float(obj)
    return str(obj)

# ==========================================
# 2. 策略引擎
# ==========================================
def analyze_stock(df, bench_df):
    try:
        df = df.replace([np.inf, -np.inf], np.nan).dropna()
        if len(df) < 150: return None
        
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        curr_price = float(close.iloc[-1])
        
        # RS评级计算
        def get_p(s, d): return (s.iloc[-1] / s.iloc[-min(len(s), d)]) - 1
        s_p = get_p(close, 250)*0.4 + get_p(close, 60)*0.3 + get_p(close, 20)*0.3
        b_p = get_p(bench_df, 250)*0.4 + get_p(bench_df, 60)*0.3 + get_p(bench_df, 20)*0.3
        rs_score = round((s_p - b_p + 1) * 85, 2)
        
        # 52W位置与枢轴
        h_52w = high.tail(250).max()
        pos_52w = (curr_price / h_52w) * 100
        pivot = float(high.tail(50).iloc[:-1].max())
        v_ratio = vol.iloc[-1] / (vol.tail(20).mean() + 1e-9)
        
        # 紧缩度 (VCP)
        r10 = (high.tail(10).max() - low.tail(10).min()) / close.tail(10).mean()
        r30 = (high.tail(30).max() - low.tail(30).min()) / close.tail(30).mean()
        is_tight = r10 < (r30 * 0.75)
        
        action, risk = "观察整理", "正常"
        if rs_score > 80 and pos_52w > 85:
            if curr_price >= pivot * 0.98 and v_ratio > 1.1:
                action, risk = "🚀 黎明枢轴(确认)", "🔥 核心突破"
            elif is_tight:
                action, risk = "👁️ 奇点先行(紧缩)", "机构吸筹"
        elif rs_score < 65:
            action, risk = "弱势震荡", "回避"
            
        return {
            "rs": rs_score, "act": action, "pos": pos_52w,
            "piv": pivot, "stop": curr_price * 0.93,
            "vol": v_ratio, "risk": risk, "tight": "✅" if is_tight else "❌"
        }
    except: return None

# ==========================================
# 3. 主程序
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"ID-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"开始执行... ID: {trace_id}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        data = yf.download(tickers, period="2y", group_by='ticker', progress=False, auto_adjust=True)
        idx_df = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench_close = idx_df['Close'].dropna()
    except Exception as e:
        print(f"下载失败: {e}"); return

    results = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            df_s = data[t_full] if len(tickers) > 1 else data
            res = analyze_stock(df_s, bench_close)
            if res:
                results.append([
                    t_raw, res['act'], f"{res['pos']:.1f}%", 
                    f"{res['piv']:.2f}", f"{res['stop']:.2f}", f"{res['vol']:.2f}", 
                    res['rs'], res['risk'], res['tight'], dt_str
                ])
        except: continue

    results.sort(key=lambda x: float(x[6]), reverse=True)

    header = [
        ["🏰 V50-Advanced Guardian", "编号:", trace_id, "策略:", "VCP+RS评级", "更新:", dt_str, "", "", ""],
        ["代码", "指令", "52W位置", "枢轴买点", "7%止损", "量能强度", "RS评级", "风险状态", "紧缩完成", "同步时刻"]
    ]
    
    try:
        payload = json.loads(json.dumps(header + results, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=25)
        print(f"同步结果: {resp.text} | 目标 ID: {trace_id}")
    except Exception as e:
        print(f"发送异常: {e}")

if __name__ == "__main__":
    main()
