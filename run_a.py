import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings, uuid
from datetime import timezone, timedelta

# 屏蔽警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请换成你“新建部署”后得到的最新 URL)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxIkSuUE-7q_FbdgbG9y06H93LlM0bmlHLYQJWJ1RRF9ljh8CFuBOzEi6ZjlXoaapQ/exec" 

CORE_TICKERS_RAW = [
    "600519", "300750", "601138", "300502", "603501", "688041", "002371", "300308",
    "002475", "002594", "601899", "600030", "600900", "600150", "300274", "000333",
    "688981", "300763", "002415", "603259", "601318", "000651", "600585", "000725"
]

def format_ticker(code):
    if code.startswith('6'): return f"{code}.SS"
    if code.startswith('0') or code.startswith('3'): return f"{code}.SZ"
    if code.startswith('688'): return f"{code}.SS"
    return f"{code}.BJ" if code.startswith('8') or code.startswith('4') else code

def safe_convert(obj):
    if isinstance(obj, (np.integer, np.floating)): return float(obj)
    return str(obj)

# ==========================================
# 2. 核心逻辑
# ==========================================
def calculate_ultimate_safe(df, bench_series, mkt_regime):
    try:
        df = df.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
        if len(df) < 120: return None
        
        c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
        curr_price = float(c.iloc[-1])
        
        # 核心指标
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        is_stage2 = curr_price > ma200 and ma50 > ma200
        
        # ATR止损
        tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(20).mean().iloc[-1]
        stop_loss = max(min(curr_price - (atr * 1.5), curr_price * 0.99), curr_price * 0.5)
        
        # 量能
        up_v = v[c > c.shift(1)].tail(10).mean()
        dn_v = v[c < c.shift(1)].tail(10).mean()
        vol_ratio = min(max(float(up_v) / (float(dn_v) + 1e-9), 0.1), 10.0)
        
        # 枢轴买点
        pivot_20d = float(h.tail(20).iloc[:-1].max())
        
        # 综合评分 (0-100)
        score = (float(curr_price)/c.iloc[-21]*20 + float(curr_price)/c.iloc[-63]*10 + vol_ratio*5)
        if not is_stage2: score *= 0.6
        score = min(max(score, 0), 100)

        action = "观察"
        if is_stage2 and curr_price >= pivot_20d * 0.98 and vol_ratio > 1.2:
            action = "🚀 枢轴突破"
        elif vol_ratio > 1.5 and curr_price > ma50:
            action = "🛡️ 机构吸筹"

        return {
            "score": score, "action": action, 
            "pivot": pivot_20d, "stop": stop_loss, 
            "vol": vol_ratio, "stage2": "✅" if is_stage2 else "❌"
        }
    except: return None

# ==========================================
# 3. 执行主程序
# ==========================================
def run_v50_safe_guard():
    # 北京时间处理
    tz_beijing = timezone(timedelta(hours=8))
    now_beijing = datetime.datetime.now(tz_beijing)
    update_time_str = now_beijing.strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"ID-{uuid.uuid4().hex[:6].upper()}"

    print(f"🚀 A-Share Screener 启动 | {trace_id} | {update_time_str}")

    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False, auto_adjust=True)
        m_idx = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        bench = m_idx['Close'].replace([np.inf, -np.inf], np.nan).dropna().squeeze()
        mkt_regime = "Bull" if bench.iloc[-1] > bench.rolling(50).mean().iloc[-1] else "Bear"
    except Exception as e:
        print(f"❌ 数据源故障: {e}"); return

    final_matrix = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            df_t = data[t_full] if len(tickers) > 1 else data
            res = calculate_ultimate_safe(df_t, bench, mkt_regime)
            
            if res:
                final_matrix.append([
                    t_raw, 
                    res['action'], 
                    res['stage2'], 
                    f"{res['pivot']:.2f}",   # 强制保留2位小数
                    f"{res['stop']:.2f}", 
                    f"{res['vol']:.2f}", 
                    round(res['score'], 2), 
                    "正常" if res['score'] < 80 else "高度关注", 
                    "A-Share", 
                    now_beijing.strftime('%H:%M:%S')
                ])
        except: continue

    final_matrix.sort(key=lambda x: float(x[6]), reverse=True)

    header = [
        ["📊 V50 A-Share Screener", "同步编号:", trace_id, "标签页:", "A-Share screener", "大盘:", mkt_regime, "更新:", update_time_str, ""],
        ["代码", "建议指令", "二阶段", "枢轴买点", "科学止损价", "量能比", "综合评分", "风险状态", "市场", "最后同步时间"]
    ]

    try:
        payload = header + final_matrix
        clean_json = json.loads(json.dumps(payload, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=clean_json, timeout=15)
        print(f"🎉 同步响应: {resp.text}")
        print(f"✅ 数据已发送至标签页 'A-Share screener'，同步ID: {trace_id}")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v50_safe_guard()
