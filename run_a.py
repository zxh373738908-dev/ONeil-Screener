import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings
from datetime import timezone, timedelta

# 屏蔽警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbzZdO1FFAZGwP3WH0_urUNlR9pePn3Sajkz9OePhpqchCfkAVmagpusGSASwrM9SLl7/exec"

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

def safe_div(n, d):
    try:
        res = float(n) / (float(d) + 1e-9)
        return res if math.isfinite(res) else 0.0
    except: return 0.0

def clamp(val, min_v, max_v):
    return max(min(val, max_v), min_v)

# ==========================================
# 2. 核心计算引擎
# ==========================================
def calculate_ultimate_safe(df, bench_series, mkt_regime):
    try:
        df = df.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
        if len(df) < 120: return None
        
        c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
        curr_price = float(c.iloc[-1])
        
        # 趋势阶段 (Stage 2)
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        is_stage2 = curr_price > ma200 and ma50 > ma200
        
        # 波动率止损 (ATR)
        tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(20).mean().iloc[-1]
        stop_loss = clamp(curr_price - (atr * 1.5), curr_price * 0.5, curr_price * 0.99)
        
        # 量能强度
        up_v = v[c > c.shift(1)].tail(10).mean()
        dn_v = v[c < c.shift(1)].tail(10).mean()
        vol_ratio = clamp(safe_div(up_v, dn_v), 0.1, 10.0)
        
        # 枢轴买点 (20日高点)
        pivot_20d = float(h.tail(20).iloc[:-1].max())
        
        # 评分计算
        perf_score = safe_div(curr_price, c.iloc[-21]) * 20 + safe_div(curr_price, c.iloc[-63]) * 10
        final_score = clamp(perf_score + (vol_ratio * 5), 0, 100)
        
        if not is_stage2: final_score *= 0.6
        if mkt_regime == "Bear": final_score *= 0.8

        action = "观察"
        if is_stage2 and curr_price >= pivot_20d * 0.98 and vol_ratio > 1.2:
            action = "🚀 枢轴突破"
        elif vol_ratio > 1.5 and curr_price > ma50:
            action = "🛡️ 机构吸筹"

        return {
            "score": round(final_score, 2), 
            "action": action, 
            "pivot": round(pivot_20d, 2), 
            "stop": round(stop_loss, 2), 
            "vol": round(vol_ratio, 2), 
            "stage2": "✅" if is_stage2 else "❌"
        }
    except: return None

# ==========================================
# 3. 主执行流程
# ==========================================
def run_v50_safe_guard():
    # 获取北京时间
    tz_beijing = timezone(timedelta(hours=8))
    now_beijing = datetime.datetime.now(tz_beijing)
    update_time_str = now_beijing.strftime('%Y-%m-%d %H:%M:%S')

    print(f"🚀 安全版 V50-Guardian 启动 | 北京时间: {update_time_str}")

    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        # 增加 auto_adjust=True 确保价格是复权后的
        data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False, auto_adjust=True)
        m_idx = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        
        if m_idx.empty:
            print("❌ 无法获取指数数据，请检查网络"); return
            
        bench = m_idx['Close'].replace([np.inf, -np.inf], np.nan).dropna().squeeze()
        mkt_regime = "Bull" if bench.iloc[-1] > bench.rolling(50).mean().iloc[-1] else "Bear"
    except Exception as e:
        print(f"❌ 数据源获取失败: {e}"); return

    final_matrix = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            df_t = data[t_full] if len(tickers) > 1 else data
            
            # 检查个股数据是否为空
            if df_t.empty or len(df_t) < 10: continue
            
            res = calculate_ultimate_safe(df_t, bench, mkt_regime)
            
            if res:
                # 强制转换数值格式，确保输出为规范的小数
                final_matrix.append([
                    t_raw, 
                    res['action'], 
                    res['stage2'], 
                    f"{res['pivot']:.2f}",   # 格式化为字符串，保留2位小数
                    f"{res['stop']:.2f}", 
                    f"{res['vol']:.2f}", 
                    round(res['score'], 2), 
                    "正常" if res['score'] < 80 else "高度关注", 
                    "A-Share", 
                    now_beijing.strftime('%H:%M:%S')
                ])
        except: continue

    # 按分数排序
    final_matrix.sort(key=lambda x: float(x[6]), reverse=True)
    
    # 构建表头
    header = [
        ["🏰 V50-SafeGuardian", "大盘状态:", mkt_regime, "更新日期:", update_time_str, "", "", "", "", ""],
        ["代码", "建议指令", "二阶段", "枢轴买点", "科学止损价", "量能比", "综合评分", "风险状态", "市场", "最后同步时间"]
    ]

    # 发送请求
    try:
        payload = header + final_matrix
        clean_json = json.loads(json.dumps(payload, default=safe_convert))
        
        resp = requests.post(WEBAPP_URL, json=clean_json, timeout=15)
        if "SUCCESS" in resp.text:
            print(f"🎉 同步成功！北京时间 {now_beijing.strftime('%H:%M')} 数据已写入 Google Sheet")
        else:
            print(f"⚠️ 脚本响应异常: {resp.text}")
    except Exception as e:
        print(f"❌ 同步过程中发生错误: {e}")

if __name__ == "__main__":
    run_v50_safe_guard()
