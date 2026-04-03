import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (使用你提供的 URL)
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
# 2. 更加鲁棒的策略引擎
# ==========================================
def analyze_stock(df, bench_df, t_name):
    try:
        # 清洗并确保有数据
        df = df.dropna(subset=['Close', 'High', 'Low'])
        if len(df) < 30: # 门槛降到30天，先确保能出数据
            print(f"  [!] {t_name} 数据太少: {len(df)} 行")
            return None
        
        close = df['Close']
        high = df['High']
        low = df['Low']
        vol = df['Volume']
        curr_price = float(close.iloc[-1])
        
        # RS评级计算 (增加安全检查)
        def get_p(s, d):
            actual_d = min(len(s), d)
            return (s.iloc[-1] / s.iloc[-actual_d]) - 1
            
        s_p = get_p(close, 250)*0.4 + get_p(close, 60)*0.3 + get_p(close, 20)*0.3
        b_p = get_p(bench_df, 250)*0.4 + get_p(bench_df, 60)*0.3 + get_p(bench_df, 20)*0.3
        rs_score = round((s_p - b_p + 1) * 85, 2)
        
        # 52W位置
        h_52w = high.max() # 2年内的最高
        pos_52w = (curr_price / h_52w) * 100
        
        # 枢轴 (近期高点)
        pivot = float(high.tail(20).iloc[:-1].max()) 
        v_ratio = vol.iloc[-1] / (vol.tail(10).mean() + 1e-9)
        
        # 紧缩检查 (VCP)
        r10 = (high.tail(10).max() - low.tail(10).min()) / (close.tail(10).mean() + 1e-9)
        r20 = (high.tail(20).max() - low.tail(20).min()) / (close.tail(20).mean() + 1e-9)
        is_tight = r10 < r20 
        
        # 指令判定
        action, risk = "观察中", "正常"
        if rs_score > 80 and pos_52w > 80:
            if curr_price >= pivot * 0.98 and v_ratio > 1.1:
                action, risk = "🚀 黎明枢轴(确认)", "🔥 核心突破"
            elif is_tight:
                action, risk = "👁️ 奇点先行(紧缩)", "机构吸筹"
        elif rs_score < 70:
            action, risk = "弱势整理", "回避"
            
        return {
            "rs": rs_score, "act": action, "pos": pos_52w,
            "piv": pivot, "stop": curr_price * 0.93,
            "vol": v_ratio, "risk": risk, "tight": "✅" if is_tight else "❌"
        }
    except Exception as e:
        print(f"  [!] {t_name} 计算出错: {e}")
        return None

# ==========================================
# 3. 主流程
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"ID-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 启动扫描 | 编号: {trace_id} | 时间: {dt_str}")
    
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        # 下载数据
        data = yf.download(tickers, period="2y", group_by='ticker', progress=False, auto_adjust=True)
        idx_df = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench_close = idx_df['Close'].dropna()
        
        if bench_close.empty:
            print("❌ 无法获取大盘数据，请检查网络"); return
        print(f"✅ 成功下载 {len(tickers)} 只个股数据")
    except Exception as e:
        print(f"❌ 下载过程发生异常: {e}"); return

    results = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            # yfinance 多索引提取逻辑
            if len(tickers) > 1:
                df_s = data[t_full]
            else:
                df_s = data
                
            if df_s is None or df_s.empty:
                print(f"  [!] {t_raw} 数据为空，跳过")
                continue
                
            res = analyze_stock(df_s, bench_close, t_raw)
            if res:
                results.append([
                    t_raw, res['act'], f"{res['pos']:.1f}%", 
                    f"{res['piv']:.2f}", f"{res['stop']:.2f}", f"{res['vol']:.2f}", 
                    res['rs'], res['risk'], res['tight'], dt_str
                ])
        except Exception as e:
            print(f"  [!] {t_full} 处理异常: {e}")
            continue

    print(f"📊 扫描结束，有效数据: {len(results)} 行")

    # 排序
    if results:
        results.sort(key=lambda x: float(x[6]), reverse=True)

    # 组合表头
    header = [
        ["🏰 V50-Advanced Guardian", "编号:", trace_id, "策略:", "VCP+RS评级", "更新:", dt_str, "", "", ""],
        ["代码", "指令", "52W位置", "枢轴买点", "7%止损", "量能强度", "RS评级", "风险状态", "紧缩完成", "同步时刻"]
    ]
    
    final_payload = header + results

    # 发送
    try:
        clean_json = json.loads(json.dumps(final_payload, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=clean_json, timeout=30)
        print(f"🎉 同步响应: {resp.text} | 写入行数: {len(results)}")
    except Exception as e:
        print(f"❌ 发送失败: {e}")

if __name__ == "__main__":
    main()
