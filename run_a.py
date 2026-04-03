import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxIkSuUE-7q_FbdgbG9y06H93LlM0bmlHLYQJWJ1RRF9ljh8CFuBOzEi6ZjlXoaapQ/exec" 

CORE_TICKERS_RAW = [
    "600519", "300750", "601138", "300502", "603501", "688041", "002371", "300308",
    "002475", "002594", "601899", "600030", "600900", "600150", "300274", "000333",
    "688981", "300763", "002415", "603259", "601318", "000651", "600585", "000725",
    "000951", "601857", "600019", "000895"
]

def format_ticker(code):
    code = code.zfill(6)
    return f"{code}.SS" if code.startswith('6') else f"{code}.SZ"

def safe_convert(obj):
    if isinstance(obj, (np.integer, np.floating)): return float(obj)
    return str(obj)

# ==========================================
# 2. 增强型策略引擎
# ==========================================
def calculate_ultimate_safe(df, bench_series):
    try:
        df = df.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
        if len(df) < 150: return None # 降低门槛到150天
        
        c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
        curr_price = float(c.iloc[-1])
        
        # A. 趋势状态
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        # 只要价格在50日线上，就算初步走强
        trend_ok = curr_price > ma50
        
        # B. RS 评级计算 (相对大盘强度)
        def get_perf(ser, days):
            idx = min(len(ser), days)
            return (ser.iloc[-1] / ser.iloc[-idx])
        
        stock_perf = (get_perf(c, 250)*0.4 + get_perf(c, 60)*0.3 + get_perf(c, 20)*0.3)
        bench_perf = (get_perf(bench_series, 250)*0.4 + get_perf(bench_series, 60)*0.3 + get_perf(bench_series, 20)*0.3)
        rs_rating = round((stock_perf / bench_perf) * 90, 2)
        
        # C. 紧缩度 (VCP思路)
        vol_10 = c.tail(10).std() / c.tail(10).mean()
        vol_30 = c.tail(30).std() / c.tail(30).mean()
        is_tight = "紧缩" if vol_10 < vol_30 else "波动"
        
        # D. 枢轴与位置
        pivot_50d = float(h.tail(50).iloc[:-1].max())
        high_52w = h.tail(250).max()
        pos_52w = (curr_price / high_52w) * 100
        
        # --- 指令判定 ---
        if rs_rating > 95 and curr_price >= pivot_50d * 0.98:
            action = "🚀 黎明枢轴(确认)"
            status = "🔥 核心推荐"
        elif rs_rating > 85 and is_tight == "紧缩":
            action = "👁️ 奇点先行(Stealth)"
            status = "关注"
        elif trend_ok:
            action = "🛡️ 蓝筹复苏(潜伏)"
            status = "持有"
        else:
            action = "弱势整理"
            status = "回避"

        return {
            "score": rs_rating, "action": action, "pivot": pivot_50d, 
            "stop": curr_price * 0.93, "vol": v.iloc[-1]/v.tail(20).mean(), 
            "pos52w": pos_52w, "status": status, "tight": is_tight
        }
    except Exception as e:
        return None

# ==========================================
# 3. 主程序
# ==========================================
def run_v50_safe_guard():
    tz_beijing = timezone(timedelta(hours=8))
    now_beijing = datetime.datetime.now(tz_beijing)
    update_time_str = now_beijing.strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"V50-{uuid.uuid4().hex[:4].upper()}"

    print(f"🚀 启动扫描 | 编号: {trace_id}")

    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=True)
        m_idx = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench = m_idx['Close'].dropna()
        print(f"✅ 成功获取 {len(tickers)} 只股票数据")
    except Exception as e:
        print(f"❌ 下载失败: {e}"); return

    final_matrix = []
    processed_count = 0
    
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            df_t = data[t_full] if len(tickers) > 1 else data
            if df_t.empty: continue
            
            res = calculate_ultimate_safe(df_t, bench)
            if res:
                processed_count += 1
                final_matrix.append([
                    t_raw, res['action'], f"{res['pos52w']:.1f}%", 
                    f"{res['pivot']:.2f}", f"{res['stop']:.2f}", f"{res['vol']:.2f}", 
                    res['score'], res['status'], res['tight'], 
                    now_beijing.strftime('%H:%M:%S')
                ])
        except: continue

    print(f"📊 筛选完成：{processed_count} 只股票符合基本上市条件")

    # 按 RS 评分排序
    final_matrix.sort(key=lambda x: float(x[6]), reverse=True)

    header = [
        ["🏰 V50-Guardian", "同步ID:", trace_id, "大盘状态:", "Active", "更新:", update_time_str, "", "", ""],
        ["代码", "选股指令", "52W位置", "枢轴买点", "7%止损位", "量能强度", "RS强度", "风险状态", "紧缩检查", "同步时间"]
    ]

    try:
        payload = header + final_matrix
        clean_json = json.loads(json.dumps(payload, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=clean_json, timeout=15)
        print(f"🎉 同步响应: {resp.text}")
    except Exception as e:
        print(f"❌ 发送失败: {e}")

if __name__ == "__main__":
    run_v50_safe_guard()
    except Exception as e:
        print(f"❌ 失败: {e}")

if __name__ == "__main__":
    run_v50_safe_guard()
