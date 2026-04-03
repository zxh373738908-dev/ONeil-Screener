import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings, uuid
from datetime import timezone, timedelta

# 彻底屏蔽警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请确保 URL 正确)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxIkSuUE-7q_FbdgbG9y06H93LlM0bmlHLYQJWJ1RRF9ljh8CFuBOzEi6ZjlXoaapQ/exec"

# 包含 000951 及其它核心蓝筹
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
        return float(obj)
    return str(obj)

# ==========================================
# 2. 策略引擎 (VCP + RS)
# ==========================================
def analyze_stock(df, bench_df):
    try:
        # 清洗数据
        df = df.replace([np.inf, -np.inf], np.nan).dropna()
        if len(df) < 150:
            return None
        
        close = df['Close']
        high = df['High']
        low = df['Low']
        vol = df['Volume']
        curr_price = float(close.iloc[-1])
        
        # A. 计算 RS 强度 (个股vs大盘)
        # 000951 型牛股 RS 必须跑赢大盘
        def get_perf(ser, days):
            d = min(len(ser), days)
            return (ser.iloc[-1] / ser.iloc[-d]) - 1
            
        s_p = get_perf(close, 250)*0.4 + get_perf(close, 60)*0.3 + get_perf(close, 20)*0.3
        b_p = get_perf(bench_df, 250)*0.4 + get_perf(bench_df, 60)*0.3 + get_perf(bench_df, 20)*0.3
        rs_score = round((s_p - b_p + 1) * 80, 2)
        
        # B. 52周高位百分比 (000951 在高位启动)
        h_52w = high.tail(250).max()
        pos_52w = (curr_price / h_52w) * 100
        
        # C. VCP 紧缩度判断 (关键：过滤假突破)
        r_10 = (high.tail(10).max() - low.tail(10).min()) / close.tail(10).mean()
        r_30 = (high.tail(30).max() - low.tail(30).min()) / close.tail(30).mean()
        # 紧缩定义：最近10天波动明显小于最近30天
        is_tight = r_10 < (r_30 * 0.75)
        
        # D. 枢轴突破
        pivot = float(high.tail(50).iloc[:-1].max())
        v_ratio = vol.iloc[-1] / (vol.tail(20).mean() + 1e-9)
        
        # --- 策略分级 ---
        action = "观察整理"
        risk = "正常"
        
        # 000951 特征：RS评级高(>80)，位置高(>85%)，形态紧缩或量能突破
        if rs_score > 80 and pos_52w > 85:
            if curr_price >= pivot * 0.98 and v_ratio > 1.2:
                action = "🚀 黎明枢轴(确认)"
                risk = "🔥 核心突破"
            elif is_tight:
                action = "👁️ 奇点先行(紧缩)"
                risk = "机构吸筹"
            else:
                action = "蓝筹复苏(高位)"
        elif rs_score < 60:
            action = "弱势震荡"
            risk = "回避"
            
        return {
            "score": rs_score, "action": action, "pos": pos_52w,
            "pivot": pivot, "stop": curr_price * 0.93,
            "vol": v_ratio, "risk": risk, "tight": "✅" if is_tight else "❌"
        }
    except:
        return None

# ==========================================
# 3. 执行主流程
# ==========================================
def main():
    # 时间设置
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"V50-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"开始执行... ID: {trace_id}")
    
    # 转换代码
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        # 1. 下载数据 (auto_adjust=True 自动处理复权)
        data = yf.download(tickers, period="2y", group_by='ticker', progress=False, auto_adjust=True)
        idx_df = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench_close = idx_df['Close'].dropna()
    except Exception as e:
        print(f"下载失败: {e}")
        return

    results = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            # 获取单只票的 DataFrame
            df_stock = data[t_full] if len(tickers) > 1 else data
            
            if df_stock.empty:
                continue
                
            res = analyze_stock(df_stock, bench_close)
            
            if res:
                # 构造一行数据 (列表格式)
                row = [
                    t_raw, res['action'], f"{res['pos']:.1f}%", 
                    f"{res['pivot']:.2f}", f"{res['stop']:.2f}", f"{res['vol']:.2f}", 
                    res['score'], res['risk'], res['tight'], dt_str
                ]
                results.append(row)
        except:
            continue

    # 按 RS 强度排序
    results.sort(key=lambda x: float(x[6]), reverse=True)

    # 构造完整 payload
    header = [
        ["🏰 V50-Advanced Guardian", "编号:", trace_id, "策略:", "VCP+RS评级", "更新:", dt_str, "", "", ""],
        ["代码", "指令", "52W位置", "枢轴买点", "7%止损", "量能强度", "RS评级", "风险状态", "紧缩完成", "同步时刻"]
    ]
    
    final_data = header + results

    # 发送数据
    try:
        clean_payload = json.loads(json.dumps(final_data, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=clean_payload, timeout=25)
        print(f"同步结果: {resp.text}")
    except Exception as e:
        print(f"发送异常: {e}")

if __name__ == "__main__":
    main()
