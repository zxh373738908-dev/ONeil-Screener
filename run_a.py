import yfinance as yf
import pandas as pd
import numpy as np
import datetime, requests, json, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxcwtfGFZqWyulM2x63ytoYnuYzR-siWVCahjsIqdRbsuYjBac8YCuy7GTRlwd-YGmc/exec"

CORE_TICKERS_RAW =[
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
        return float(obj) if not np.isnan(obj) else 0.0
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
# 2. V50.1 缩量毒蛇引擎 (反量化伏击)
# ==========================================
def analyze_stock_viper(df_in, bench_ser, t_name):
    try:
        close = get_safe_col(df_in, 'Close').astype(float)
        high = get_safe_col(df_in, 'High').astype(float)
        low = get_safe_col(df_in, 'Low').astype(float)
        vol = get_safe_col(df_in, 'Volume').astype(float)
        
        if close.empty or len(close) < 250: return None
        curr_price = float(close.iloc[-1])
        prev_close = float(close.iloc[-2])
        
        # --- A. 强者恒强底色 (RS评级) ---
        def get_p(s, d): return float((s.iloc[-1] / s.iloc[-d]) - 1)
        s_p = get_p(close, 250)*0.4 + get_p(close, 60)*0.3 + get_p(close, 20)*0.3
        b_p = get_p(bench_ser, 250)*0.4 + get_p(bench_ser, 60)*0.3 + get_p(bench_ser, 20)*0.3
        rs_score = round((s_p - b_p + 1) * 85, 2)
        
        # --- B. 黄金口袋 (距最高点回调深度) ---
        # 寻找近期（约1个月）最高点
        recent_high = float(high.tail(22).max())
        dist_high_pct = ((curr_price / recent_high) - 1) * 100 
        
        # --- C. 绝对地量 (量化休眠) ---
        # 使用前20个交易日均量作为基准（不包含今天）
        avg_vol_20 = vol.iloc[-21:-1].mean()
        vol_ratio = float(vol.iloc[-1] / avg_vol_20) if avg_vol_20 > 0 else 1.0
        
        # --- D. 极致紧缩 (日内振幅) ---
        # 振幅 = (最高 - 最低) / 昨收
        amplitude = ((high.iloc[-1] - low.iloc[-1]) / prev_close) * 100
        
        # --- E. 均线蹦床 (生命线托底) ---
        ma20 = close.rolling(20).mean().iloc[-1]
        ma50 = close.rolling(50).mean().iloc[-1]
        
        # 计算距离均线的乖离（只允许在上方 0% 到 1.5% 之间，精准踩线）
        dist_ma20 = (curr_price - ma20) / ma20 * 100
        dist_ma50 = (curr_price - ma50) / ma50 * 100
        
        on_ma20 = 0 <= dist_ma20 <= 1.5
        on_ma50 = 0 <= dist_ma50 <= 1.5
        ma_support_str =[]
        if on_ma20: ma_support_str.append("MA20")
        if on_ma50: ma_support_str.append("MA50")
        ma_status = "+".join(ma_support_str) if ma_support_str else "悬空/跌破"

        # ==========================================
        # 🎯 毒蛇判定逻辑 🎯
        # ==========================================
        action = "潜伏观察"
        is_leader = rs_score >= 80
        is_golden_pit = -12 <= dist_high_pct <= -4
        is_sleep_vol = vol_ratio < 0.65
        is_tight = amplitude < 5.0
        is_ma_support = on_ma20 or on_ma50

        if is_leader and is_golden_pit:
            if is_sleep_vol and is_tight and is_ma_support:
                action = "🐍 毒蛇出洞 (满血击杀)"
            elif is_sleep_vol and is_ma_support:
                action = "⚠️ 猎物入圈 (准备)"
            elif is_sleep_vol:
                action = "📉 缩量深蹲 (无均线)"
            else:
                action = "洗盘未尽 (量大)"
        elif is_leader and dist_high_pct > -4:
             action = "🔥 高位震荡 (禁追)"
        elif dist_high_pct < -12:
             action = "💀 破位深跌 (回避)"
            
        return {
            "rs": rs_score, 
            "act": action, 
            "dist": dist_high_pct,
            "vol_r": vol_ratio, 
            "tight": amplitude, 
            "ma_stat": ma_status
        }
    except Exception as e:
        return None

# ==========================================
# 3. 主流程
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"VIPER-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🐍 V50.1 缩量毒蛇扫描开始 | ID: {trace_id} | {dt_str}")
    tickers =[format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        print(f"⏳ 正在下载数据...")
        data = yf.download(tickers, period="2y", group_by='ticker', progress=False, auto_adjust=True)
        idx_df = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench_close = get_safe_col(idx_df, 'Close')
        print(f"✅ 数据下载完成。")
    except Exception as e:
        print(f"❌ 数据获取错误: {e}"); return

    results =[]
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            df_s = data[t_full] if len(tickers) > 1 else data
            res = analyze_stock_viper(df_s, bench_close, t_raw)
            if res:
                results.append([
                    t_raw, 
                    res['act'], 
                    f"{res['dist']:.2f}%",   # 回撤深度 (-12% ~ -4%)
                    f"{res['vol_r']:.2f}x",  # 量比 (<0.65)
                    f"{res['tight']:.2f}%",  # 振幅/紧致度 (<5)
                    res['ma_stat'],          # 均线踩踏情况
                    res['rs'],               # 强者恒强评级
                    "✅ 达标" if "毒蛇" in res['act'] else "❌ 过滤", 
                    dt_str
                ])
        except Exception as e:
            continue

    # 按回撤深度排序，最接近 -12% 黄金坑的排前面，且优先展示“毒蛇出洞”
    results.sort(key=lambda x: (1 if "毒蛇" in x[1] else 0, float(x[6])), reverse=True)

    header = [["🐍 V50.1 缩量毒蛇", "ID:", trace_id, "策略:", "专抓黄金坑底 (左侧缩量伏击)", "更新:", dt_str, "", ""],["代码", "交易指令", "距近期高点", "今日量比(萎缩)", "今日振幅(紧缩)", "均线蹦床", "RS评级", "终极判定", "北京时间"]
    ]
    
    try:
        payload = json.loads(json.dumps(header + results, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"🎉 毒蛇信号推送完成: {resp.status_code}")
    except Exception as e:
        print(f"❌ 推送失败: {e}")

if __name__ == "__main__":
    main()
