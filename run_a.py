import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings, uuid
from datetime import timezone, timedelta

# 屏蔽警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
# 请确保使用“新建部署”后生成的以 /exec 结尾的 URL
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxIkSuUE-7q_FbdgbG9y06H93LlM0bmlHLYQJWJ1RRF9ljh8CFuBOzEi6ZjlXoaapQ/exec" 

CORE_TICKERS_RAW = [
    "600519", "300750", "601138", "300502", "603501", "688041", "002371", "300308",
    "002475", "002594", "601899", "600030", "600900", "600150", "300274", "000333",
    "688981", "300763", "002415", "603259", "601318", "000651", "600585", "000725",
    "000951", "601857", "600019", "000895"
]

def format_ticker(code):
    code = str(code).zfill(6)
    return f"{code}.SS" if code.startswith('6') else f"{code}.SZ"

def safe_convert(obj):
    if isinstance(obj, (np.integer, np.floating)): return float(obj)
    return str(obj)

# ==========================================
# 2. O'Neil + VCP 策略核心
# ==========================================
def calculate_oneil_vcp(df, bench_series):
    try:
        df = df.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
        if len(df) < 150: return None
        
        c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
        curr_price = float(c.iloc[-1])
        
        # A. 相对强度评级 (RS Score) - 模拟 O'Neil 1-99 评分
        def get_perf(ser, days):
            d = min(len(ser), days)
            return (ser.iloc[-1] / ser.iloc[-d]) - 1
            
        # 加权计算表现 (最近表现权重更高)
        s_perf = get_perf(c, 250)*0.4 + get_perf(c, 120)*0.2 + get_perf(c, 60)*0.2 + get_perf(c, 21)*0.2
        b_perf = get_perf(bench_series, 250)*0.4 + get_perf(bench_series, 120)*0.2 + get_perf(bench_series, 60)*0.2 + get_perf(bench_series, 21)*0.2
        
        rs_score = round((s_perf - b_perf + 1) * 70, 2) # 归一化

        # B. 52周高位位置 (过滤垃圾股反弹)
        high_52w = h.tail(250).max()
        pos_52w = (curr_price / high_52w) * 100
        
        # C. VCP 紧缩特性检查 (核心：过滤假突破)
        # 过去10天波幅 < 过去30天波幅 = 紧缩完成
        vol_10 = (h.tail(10).max() - l.tail(10).min()) / c.tail(10).mean()
        vol_30 = (h.tail(30).max() - l.tail(30).min()) / c.tail(30).mean()
        is_tight = vol_10 < (vol_30 * 0.8) # 10天波幅明显窄于30天

        # D. 枢轴与量能
        pivot_50d = float(h.tail(50).iloc[:-1].max())
        vol_ratio = v.iloc[-1] / v.tail(20).mean()

        # --- 选股指令决策 ---
        action = "蓝筹复苏(潜伏)"
        status = "正常"
        
        # 000951 模式：RS强 + 位置高 + 突破/紧缩
        if rs_score > 80 and pos_52w > 80:
            if curr_price >= pivot_50d * 0.98 and vol_ratio > 1.2:
                action = "🚀 黎明枢轴(确认)"
                status = "🔥 核心突破"
            elif is_tight:
                action = "👁️ 奇点先行(紧缩)"
                status = "机构吸筹"
        elif rs_score < 60:
            action = "弱势整理"
            status = "回避"

        return {
            "score": rs_score, "action": action, "pos52w": pos_52w,
            "pivot": pivot_50d, "stop": curr_price * 0.93, 
            "vol": vol_ratio, "status": status, "tight": "✅" if is_tight else "❌"
        }
    except:
        return None

# ==========================================
# 3. 主程序
# ==========================================
def run_v50_advanced():
    tz_beijing = timezone(timedelta(hours=8))
    now_beijing = datetime.datetime.now(tz_beijing)
    update_time_str = now_beijing.strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"ID-{uuid.uuid4().hex[:4].upper()}"

    print(f"🚀 O'Neil 策略扫描启动 | 编号: {trace_id}")

    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        # 下载数据
        data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=True)
        m_idx = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench = m_idx['Close'].dropna()
    except Exception as e:
        print(f"❌ 数据获取失败: {e}")
        return

    final_matrix = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            df_t = data[t_full] if len(tickers) > 1 else data
            if df_t.empty: continue
            
            res = calculate_oneil_vcp(df_t, bench)
            if res:
                final_matrix.append([
                    t_raw, res['action'], f"{res['pos52w']:.1f}%", 
                    f"{res['pivot']:.2f}", f"{res['stop']:.2f}", f"{res['vol']:.2f}", 
                    res['score'], res['status'], res['tight'], 
                    now_beijing.strftime('%H:%M:%S')
                ])
        except:
            continue

    # 排序：RS得分从高到低
    final_matrix.sort(key=lambda x: float(x[6]), reverse=True)

    header = [
        ["🏰 V50-Advanced Screener", "同步ID:", trace_id, "策略:", "VCP+RS评级", "更新时间:", update_time_str, "", "", ""],
        ["代码", "选股指令", "52W位置", "枢轴买点", "7%止损位", "量能强度", "RS评级", "风险状态", "紧缩状态", "更新时刻"]
    ]

    try:
        payload = header + final_matrix
        clean_json = json.loads(json.dumps(payload, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=clean_json, timeout=20)
        print(f"🎉 同步响应: {resp.text}")
    except Exception as e:
        print(f"❌ 发送请求失败: {e}")

if __name__ == "__main__":
    run_v50_advanced()
    except Exception as e:
        print(f"❌ 失败: {e}")

if __name__ == "__main__":
    run_v50_safe_guard()
