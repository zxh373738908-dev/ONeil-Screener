import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
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

# 辅助函数：安全提取 Close/High/Low/Vol 列，无视 Index 深度
def get_safe_col(df, key):
    # 如果是多级索引，先尝试 xs 提取
    if isinstance(df.columns, pd.MultiIndex):
        try: return df.xs(key, axis=1, level=1).squeeze()
        except: pass
        try: return df.xs(key, axis=1, level=0).squeeze()
        except: pass
    
    # 模糊搜索匹配
    for c in df.columns:
        if key.lower() in str(c).lower():
            return df[c].squeeze()
    return pd.Series()

# ==========================================
# 2. 策略引擎
# ==========================================
def analyze_stock(df_in, bench_ser, t_name):
    try:
        close = get_safe_col(df_in, 'Close').astype(float)
        high = get_safe_col(df_in, 'High').astype(float)
        low = get_safe_col(df_in, 'Low').astype(float)
        vol = get_safe_col(df_in, 'Volume').astype(float)
        
        if close.empty or len(close) < 30: return None
        curr_price = float(close.iloc[-1])
        
        # RS 评级 (个股 vs 大盘)
        def get_p(s, d):
            actual_d = min(len(s), d)
            return float((s.iloc[-1] / s.iloc[-actual_d]) - 1)
            
        s_p = get_p(close, 250)*0.4 + get_p(close, 60)*0.3 + get_p(close, 20)*0.3
        b_p = get_p(bench_ser, 250)*0.4 + get_p(bench_ser, 60)*0.3 + get_p(bench_ser, 20)*0.3
        rs_score = round((s_p - b_p + 1) * 85, 2)
        
        # 52W位置
        h_max = float(high.max())
        pos_52w = float((curr_price / h_max) * 100)
        
        # 枢轴
        pivot = float(high.tail(20).iloc[:-1].max())
        v_ratio = float(vol.iloc[-1] / (vol.tail(10).mean() + 1e-9))
        
        # 紧缩度 (VCP)
        r10 = float((high.tail(10).max() - low.tail(10).min()) / (close.tail(10).mean() + 1e-9))
        r20 = float((high.tail(20).max() - low.tail(20).min()) / (close.tail(20).mean() + 1e-9))
        is_tight = bool(r10 < r20)
        
        action, risk = "观察中", "正常"
        if rs_score > 80 and pos_52w > 82:
            if curr_price >= pivot * 0.98 and v_ratio > 1.1:
                action, risk = "🚀 黎明枢轴(确认)", "🔥 核心突破"
            elif is_tight:
                action, risk = "👁️ 奇点先行(紧缩)", "机构吸筹"
            else:
                action = "蓝筹复苏(潜伏)"
        elif rs_score < 70:
            action, risk = "弱势整理", "回避"
            
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
    trace_id = f"V50-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 启动扫描 | 编号: {trace_id} | {dt_str}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        # 下载股票数据
        data = yf.download(tickers, period="2y", group_by='ticker', progress=False, auto_adjust=True)
        # 下载大盘数据
        idx_df = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        
        if idx_df.empty:
            print("❌ 指数下载失败"); return
            
        # 安全提取大盘 Close
        bench_close = get_safe_col(idx_df, 'Close')
        if bench_close.empty:
            print("❌ 无法提取指数 Close 列"); return
            
        print(f"✅ 数据准备就绪，开始分析...")
    except Exception as e:
        print(f"❌ 下载过程出错: {e}"); return

    results = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
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
        ["🏰 V50-Advanced Guardian", "编号:", trace_id, "策略:", "VCP+RS评级", "更新时间:", dt_str, "", "", ""],
        ["代码", "选股指令", "52W位置", "枢轴买点", "7%止损位", "量能强度", "RS评级强度", "风险状态", "紧缩完成", "北京时间"]
    ]
    
    try:
        clean_json = json.loads(json.dumps(header + results, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=clean_json, timeout=30)
        print(f"🎉 同步响应: {resp.text}")
    except Exception as e:
        print(f"❌ 发送异常: {e}")

if __name__ == "__main__":
    main()
