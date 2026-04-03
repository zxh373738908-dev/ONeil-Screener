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

def get_safe_col(df, key):
    if isinstance(df.columns, pd.MultiIndex):
        try: return df.xs(key, axis=1, level=1).squeeze()
        except: pass
        try: return df.xs(key, axis=1, level=0).squeeze()
        except: pass
    for c in df.columns:
        if key.lower() in str(c).lower(): return df[c].squeeze()
    return pd.Series()

# ==========================================
# 2. 增强型策略引擎 (V50-Pro)
# ==========================================
def analyze_stock_pro(df_in, bench_ser, t_name):
    try:
        # 基础列提取
        close = get_safe_col(df_in, 'Close').astype(float)
        high = get_safe_col(df_in, 'High').astype(float)
        low = get_safe_col(df_in, 'Low').astype(float)
        vol = get_safe_col(df_in, 'Volume').astype(float)
        
        if close.empty or len(close) < 200: return None
        curr_price = float(close.iloc[-1])
        
        # --- A. 趋势模板 (Trend Template) ---
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        # 核心条件：股价在均线上，均线多头排列
        is_stage2 = curr_price > ma200.iloc[-1] and ma50.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1]
        
        # --- B. RS 评级与位置 ---
        def get_p(s, d): return float((s.iloc[-1] / s.iloc[-min(len(s), d)]) - 1)
        s_p = get_p(close, 250)*0.4 + get_p(close, 60)*0.3 + get_p(close, 20)*0.3
        b_p = get_p(bench_ser, 250)*0.4 + get_p(bench_ser, 60)*0.3 + get_p(bench_ser, 20)*0.3
        rs_score = round((s_p - b_p + 1) * 85, 2)
        
        pos_52w = float((curr_price / high.tail(250).max()) * 100)
        pivot = float(high.tail(20).iloc[:-1].max())
        
        # --- C. VCP 紧缩与过热检查 ---
        # 1. 紧缩度
        r10 = (high.tail(10).max() - low.tail(10).min()) / close.tail(10).mean()
        r30 = (high.tail(30).max() - low.tail(30).min()) / close.tail(30).mean()
        is_tight = r10 < (r30 * 0.75)
        
        # 2. 乖离率 (防止追高)
        ext_50 = (curr_price / ma50.iloc[-1] - 1) * 100
        is_overextended = ext_50 > 25  # 距离50日线超过25%定义为过热
        
        # --- D. 动态 ATR 止损 ---
        tr = pd.concat([high-low, (high-close.shift()).abs(), (low-close.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        atr_stop = curr_price - (atr * 1.5) # 使用1.5倍ATR作为动态止损位
        
        # --- 决策逻辑 ---
        action, risk = "观察中", "正常"
        
        if rs_score > 85 and pos_52w > 85 and is_stage2:
            if is_overextended:
                action, risk = "⚠️ 涨幅过大(不追)", "过热"
            elif curr_price >= pivot * 0.98 and (vol.iloc[-1] > vol.tail(10).mean()):
                action, risk = "🚀 黎明枢轴(确认)", "🔥 核心突破"
            elif is_tight:
                action, risk = "👁️ 奇点先行(紧缩)", "机构吸筹"
            else:
                action = "二阶段持续"
        elif not is_stage2:
            action, risk = "趋势待定", "回避"
            
        return {
            "rs": rs_score, "act": action, "pos": pos_52w,
            "piv": pivot, "stop": atr_stop, "ext": ext_50,
            "risk": risk, "tight": "✅" if is_tight else "❌"
        }
    except Exception as e:
        return None

# ==========================================
# 3. 主流程
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"PRO-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 V50-Pro 扫描 | {trace_id} | {dt_str}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        data = yf.download(tickers, period="2y", group_by='ticker', progress=False, auto_adjust=True)
        idx_df = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench_close = get_safe_col(idx_df, 'Close')
        print(f"✅ 数据准备就绪...")
    except Exception as e:
        print(f"❌ 错误: {e}"); return

    results = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            df_s = data[t_full] if len(tickers) > 1 else data
            res = analyze_stock_pro(df_s, bench_close, t_raw)
            if res:
                results.append([
                    t_raw, res['act'], f"{res['pos']:.1f}%", 
                    f"{res['piv']:.2f}", f"{res['stop']:.2f}", f"{res['ext']:.1f}%", 
                    res['rs'], res['risk'], res['tight'], dt_str
                ])
        except: continue

    results.sort(key=lambda x: float(x[6]), reverse=True)

    header = [
        ["🏆 V50-Advanced Pro", "ID:", trace_id, "策略:", "二阶段趋势+ATR止损", "更新:", dt_str, "", "", ""],
        ["代码", "选股建议", "52W位置", "枢轴买点", "ATR止损位", "50日乖离率", "RS评级", "风险状态", "紧缩完成", "北京时间"]
    ]
    
    try:
        payload = json.loads(json.dumps(header + results, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"🎉 同步完成: {resp.text}")
    except Exception as e:
        print(f"❌ 失败: {e}")

if __name__ == "__main__":
    main()
