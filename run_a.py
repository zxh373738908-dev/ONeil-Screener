import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (URL 已更新)
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
# 2. 策略引擎 (强制数值化处理)
# ==========================================
def analyze_stock(df_in, bench_ser, t_name):
    try:
        # 强制转换为一维 Series
        close = df_in['Close'].squeeze().astype(float)
        high = df_in['High'].squeeze().astype(float)
        low = df_in['Low'].squeeze().astype(float)
        vol = df_in['Volume'].squeeze().astype(float)
        
        if len(close) < 30: return None
        curr_price = float(close.iloc[-1])
        
        # RS评级计算
        def get_p(s, d):
            actual_d = min(len(s), d)
            return float((s.iloc[-1] / s.iloc[-actual_d]) - 1)
            
        # 确保 bench_ser 也是 Series
        bench_s = bench_ser.squeeze().astype(float)
        
        s_p = get_p(close, 250)*0.4 + get_p(close, 60)*0.3 + get_p(close, 20)*0.3
        b_p = get_p(bench_s, 250)*0.4 + get_p(bench_s, 60)*0.3 + get_p(bench_s, 20)*0.3
        rs_score = round((s_p - b_p + 1) * 85, 2)
        
        # 位置与枢轴
        h_52w = float(high.max())
        pos_52w = float((curr_price / h_52w) * 100)
        pivot = float(high.tail(20).iloc[:-1].max()) 
        v_ratio = float(vol.iloc[-1] / (vol.tail(10).mean() + 1e-9))
        
        # 紧缩度 (scalar 比较)
        r10 = float((high.tail(10).max() - low.tail(10).min()) / (close.tail(10).mean() + 1e-9))
        r20 = float((high.tail(20).max() - low.tail(20).min()) / (close.tail(20).mean() + 1e-9))
        is_tight_bool = bool(r10 < r20)
        
        # 指令判定
        action, risk = "观察整理", "正常"
        if rs_score > 80 and pos_52w > 80:
            if curr_price >= pivot * 0.98 and v_ratio > 1.1:
                action, risk = "🚀 黎明枢轴(确认)", "🔥 核心突破"
            elif is_tight_bool:
                action, risk = "👁️ 奇点先行(紧缩)", "机构吸筹"
            else:
                action = "蓝筹复苏(潜伏)"
        elif rs_score < 70:
            action, risk = "弱势整理", "回避"
            
        return {
            "rs": rs_score, "act": action, "pos": pos_52w,
            "piv": pivot, "stop": curr_price * 0.93,
            "vol": v_ratio, "risk": risk, "tight": "✅" if is_tight_bool else "❌"
        }
    except Exception as e:
        # print(f"  [!] {t_name} 内部错误: {e}") # 调试用
        return None

# ==========================================
# 3. 执行流程
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"ID-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 启动扫描 | 编号: {trace_id} | {dt_str}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        # 核心：一次性下载所有数据，减少 API 调用次数
        data = yf.download(tickers, period="2y", group_by='ticker', progress=False, auto_adjust=True)
        idx_df = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        
        # 强制处理指数为 Series
        if isinstance(idx_df.columns, pd.MultiIndex):
            bench_close = idx_df.xs('Close', axis=1, level=1).squeeze()
        else:
            bench_close = idx_df['Close'].squeeze()
            
        print(f"✅ 数据下载成功，准备计算...")
    except Exception as e:
        print(f"❌ 下载失败: {e}"); return

    results = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            # 兼容多股和单股下载结构
            df_s = data[t_full] if len(tickers) > 1 else data
            
            if df_s.empty: continue
                
            res = analyze_stock(df_s, bench_close, t_raw)
            if res:
                results.append([
                    t_raw, res['act'], f"{res['pos']:.1f}%", 
                    f"{res['piv']:.2f}", f"{res['stop']:.2f}", f"{res['vol']:.2f}", 
                    res['rs'], res['risk'], res['tight'], dt_str
                ])
        except: continue

    print(f"📊 扫描结束，有效数据: {len(results)} 行")

    if results:
        results.sort(key=lambda x: float(x[6]), reverse=True)

    header = [
        ["🏰 V50-Advanced Guardian", "编号:", trace_id, "策略:", "VCP+RS评级", "更新:", dt_str, "", "", ""],
        ["代码", "指令", "52W位置", "枢轴买点", "7%止损", "量能强度", "RS评级", "风险状态", "紧缩完成", "同步时刻"]
    ]
    
    try:
        final_data = header + results
        clean_json = json.loads(json.dumps(final_data, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=clean_json, timeout=30)
        print(f"🎉 同步结果: {resp.text}")
    except Exception as e:
        print(f"❌ 发送异常: {e}")

if __name__ == "__main__":
    main()
