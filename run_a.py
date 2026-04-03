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
# 2. O'Neil + VCP 核心计算引擎
# ==========================================
def calculate_advanced_logic(df, bench_series):
    try:
        # 数据清洗
        df = df.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
        if len(df) < 200: return None
        
        c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
        curr_price = float(c.iloc[-1])
        
        # A. RS 相对强度评级 (模拟欧奈尔评分)
        def get_perf(ser, days):
            d = min(len(ser), days)
            return (ser.iloc[-1] / ser.iloc[-d]) - 1
            
        s_perf = get_perf(c, 250)*0.4 + get_perf(c, 63)*0.3 + get_perf(c, 21)*0.3
        b_perf = get_perf(bench_series, 250)*0.4 + get_perf(bench_series, 63)*0.3 + get_perf(bench_series, 21)*0.3
        
        # RS分：大盘表现为基准，高于大盘则分数高
        rs_rating = round((s_perf - b_perf + 1) * 85, 2)

        # B. 52周位置 (过滤底部假反弹)
        high_52w = h.tail(250).max()
        pos_52w = (curr_price / high_52w) * 100
        
        # C. VCP 紧缩特性 (过滤宽幅波动的噪音)
        # 计算过去10天与过去30天的高低波幅
        range_10 = (h.tail(10).max() - l.tail(10).min()) / c.tail(10).mean()
        range_30 = (h.tail(30).max() - l.tail(30).min()) / c.tail(30).mean()
        is_tight = range_10 < (range_30 * 0.75) # 10天波幅比30天收窄25%以上

        # D. 枢轴买点 (50日最高价)
        pivot_50d = float(h.tail(50).iloc[:-1].max())
        vol_ratio = v.iloc[-1] / (v.tail(20).mean() + 1e-9)

        # --- 决策逻辑 ---
        # 选出 000951 这种：强趋势(RS>85) + 靠近高位(Pos>85%) + 突破或紧缩
        action = "蓝筹复苏(潜伏)"
        risk_status = "正常"
        
        if rs_rating > 85 and pos_52w > 85:
            if curr_price >= pivot_50d * 0.98 and vol_ratio > 1.2:
                action = "🚀 黎明枢轴(确认)"
                risk_status = "🔥 核心突破"
            elif is_tight:
                action = "👁️ 奇点先行(紧缩)"
                risk_status = "机构洗筹"
        elif rs_rating < 65:
            action = "弱势整理"
            risk_status = "忽略"

        return {
            "score": rs_rating, "action": action, "pos52w": pos_52w,
            "pivot": pivot_50d, "stop": curr_price * 0.93, 
            "vol": vol_ratio, "status": risk_status, "tight": "✅" if is_tight else "❌"
        }
    except:
        return None

# ==========================================
# 3. 主执行流程
# ==========================================
def run_main():
    tz_beijing = timezone(timedelta(hours=8))
    now_beijing = datetime.datetime.now(tz_beijing)
    update_time_str = now_beijing.strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"V50-{uuid.uuid4().hex[:4].upper()}"

    print(f"🚀 VCP+RS 扫描开始 | ID: {trace_id}")

    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=True)
        m_idx = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench = m_idx['Close'].dropna()
        print(f"✅ 成功获取数据，大盘样本数: {len(bench)}")
    except Exception as e:
        print(f"❌ 关键数据下载失败: {e}")
        return

    final_matrix = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            # 处理单股或多股数据结构
            df_t = data[t_full] if len(tickers) > 1 else data
            if df_t.empty: continue
            
            res = calculate_advanced_logic(df_t, bench)
            if res:
                final_matrix.append([
                    t_raw, res['action'], f"{res['pos52w']:.1f}%", 
                    f"{res['pivot']:.2f}", f"{res['stop']:.2f}", f"{res['vol']:.2f}", 
                    res['score'], res['status'], res['tight'], 
                    now_beijing.strftime('%H:%M:%S')
                ])
        except Exception:
            continue

    # 按 RS 评分从高到低排序
    final_matrix.sort(key=lambda x: float(x[6]), reverse=True)

    header = [
        ["🏰 V50-Advanced Guardian", "同步ID:", trace_id, "策略:", "VCP紧缩+RS强度", "更新时刻:", update_time_str, "", "", ""],
        ["代码", "选股指令", "52W位置", "枢轴点", "7%止损位", "量能强度", "RS评级", "风险状态", "紧缩完成", "北京时间"]
    ]

    try:
        payload = header + final_matrix
        clean_json = json.loads(json.dumps(payload, default=safe_convert))
        # 发送 POST 请求
        resp = requests.post(WEBAPP_URL, json=clean_json, timeout=20)
        print(f"🎉 同步响应: {resp.text}")
    except Exception as e:
        print(f"❌ 远程同步失败: {e}")

if __name__ == "__main__":
    run_main()
    except Exception as e:
        print(f"❌ 失败: {e}")

if __name__ == "__main__":
    run_v50_safe_guard()
