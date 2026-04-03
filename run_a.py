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
    "000951", "601857", "600019", "000895" # 增加了一些典型蓝筹供测试
]

def format_ticker(code):
    code = code.zfill(6)
    return f"{code}.SS" if code.startswith('6') else f"{code}.SZ"

def safe_convert(obj):
    if isinstance(obj, (np.integer, np.floating)): return float(obj)
    return str(obj)

# ==========================================
# 2. 深度选股引擎 (合理性过滤)
# ==========================================
def calculate_ultimate_safe(df, bench_series):
    try:
        df = df.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
        if len(df) < 250: return None # 必须有1年数据
        
        c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
        curr_price = float(c.iloc[-1])
        
        # --- A. 趋势底线 (过滤垃圾股) ---
        ma50 = c.rolling(50).mean()
        ma150 = c.rolling(150).mean()
        ma200 = c.rolling(200).mean()
        # 条件：股价在200日线上，且200日线不向下掉头
        trend_ok = curr_price > ma200.iloc[-1] and ma200.iloc[-1] > ma200.iloc[-20]
        
        # --- B. RS 评级 (相对强度) ---
        # 计算逻辑：(1年收益*0.4 + 半年收益*0.2 + 3个月*0.2 + 1个月*0.2)
        def get_perf(ser, days): return (ser.iloc[-1] / ser.iloc[-min(len(ser), days)])
        stock_score = (get_perf(c, 250)*0.4 + get_perf(c, 120)*0.2 + get_perf(c, 60)*0.2 + get_perf(c, 20)*0.2)
        bench_score = (get_perf(bench_series, 250)*0.4 + get_perf(bench_series, 120)*0.2 + get_perf(bench_series, 60)*0.2 + get_perf(bench_series, 20)*0.2)
        rs_rating = round((stock_score / bench_score) * 80, 2) # 基准分为80
        
        # --- C. VCP 紧缩检查 (过滤大幅波动的假突破) ---
        # 计算过去10天波动率和过去30天波动率的比值
        volatility_10 = (c.tail(10).std() / c.tail(10).mean())
        volatility_30 = (c.tail(30).std() / c.tail(30).mean())
        is_tight = volatility_10 < volatility_30 # 波动在收缩
        
        # --- D. 量能确认 (量能强度) ---
        avg_vol = v.tail(20).mean()
        relative_vol = v.iloc[-1] / avg_vol # 今日量比
        
        # --- E. 枢轴位置 (50日高点) ---
        pivot_50d = float(h.tail(50).iloc[:-1].max())
        dist_to_pivot = (curr_price / pivot_50d) - 1
        
        # --- F. 52周高位百分比 ---
        high_52w = h.tail(250).max()
        pos_52w = (curr_price / high_52w) * 100

        # --- 决策逻辑 ---
        action = "蓝筹复苏(潜伏)"
        status = "观察"
        
        # 核心筛选条件：RS评级要高，52周位置要靠上，趋势要稳
        if rs_rating > 85 and pos_52w > 80 and trend_ok:
            if curr_price >= pivot_50d * 0.98 and relative_vol > 1.2:
                action = "🚀 黎明枢轴(确认)"
                status = "🔥 重点关注"
            elif is_tight:
                action = "👁️ 奇点先行(紧缩)"
                status = "正常"
        
        # 排除掉位置太低、RS太弱的假突破
        if pos_52w < 60 or rs_rating < 70:
            action = "弱势整理"
            status = "忽略"

        return {
            "score": rs_rating, "action": action, "pivot": pivot_50d, 
            "stop": curr_price * 0.94, "vol": relative_vol, 
            "pos52w": pos_52w, "status": status
        }
    except: return None

# ==========================================
# 3. 主执行流程
# ==========================================
def run_v50_safe_guard():
    tz_beijing = timezone(timedelta(hours=8))
    now_beijing = datetime.datetime.now(tz_beijing)
    update_time_str = now_beijing.strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"V50-{uuid.uuid4().hex[:4].upper()}"

    print(f"🚀 开始策略扫描 | 编号: {trace_id} | {update_time_str}")

    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        # 下载 2 年数据以计算 52 周位置
        data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=True)
        m_idx = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench = m_idx['Close'].dropna()
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    final_matrix = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            df_t = data[t_full] if len(tickers) > 1 else data
            res = calculate_ultimate_safe(df_t, bench)
            
            if res:
                final_matrix.append([
                    t_raw, res['action'], f"{res['pos52w']:.1f}%", 
                    f"{res['pivot']:.2f}", f"{res['stop']:.2f}", f"{res['vol']:.2f}", 
                    res['score'], res['status'], "A-Share", 
                    now_beijing.strftime('%H:%M:%S')
                ])
        except: continue

    # 排序：RS评级最高排前面
    final_matrix.sort(key=lambda x: float(x[6]), reverse=True)

    header = [
        ["📊 V50-Advanced Screener", "同步ID:", trace_id, "策略:", "RS+VCP紧缩", "更新:", update_time_str, "", "", ""],
        ["代码", "选股指令", "52W位置", "枢轴买点", "7%止损位", "量能强度", "RS评级", "风险状态", "市场", "同步时间"]
    ]

    try:
        payload = header + final_matrix
        clean_json = json.loads(json.dumps(payload, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=clean_json, timeout=15)
        print(f"🎉 同步响应: {resp.text}")
    except Exception as e:
        print(f"❌ 失败: {e}")

if __name__ == "__main__":
    run_v50_safe_guard()
