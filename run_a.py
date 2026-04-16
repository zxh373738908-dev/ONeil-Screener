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
# 2. V50.1 缩量毒蛇引擎 (绝望深坑+生命线测谎)
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
        
        # --- A. 大将底色 (只做真龙) ---
        def get_p(s, d): return float((s.iloc[-1] / s.iloc[-d]) - 1)
        s_p = get_p(close, 250)*0.4 + get_p(close, 60)*0.3 + get_p(close, 20)*0.3
        b_p = get_p(bench_ser, 250)*0.4 + get_p(bench_ser, 60)*0.3 + get_p(bench_ser, 20)*0.3
        rs_score = round((s_p - b_p + 1) * 85, 2)
        
        # --- B. 绝望的深度 (黄金坑位 -15% 到 -8%) ---
        recent_high = float(high.tail(22).max()) # 近一个月最高点
        dist_high_pct = ((curr_price / recent_high) - 1) * 100 
        
        # --- C. 绝对窒息 (引信核武 < 0.6) ---
        # 使用前20个交易日均量作为基准（不包含今天）
        avg_vol_20 = vol.iloc[-21:-1].mean()
        vol_ratio = float(vol.iloc[-1] / avg_vol_20) if avg_vol_20 > 0 else 1.0
        
        # --- D. 均线蹦床 (50日生命线精准托底) ---
        ma50 = close.rolling(50).mean().iloc[-1]
        
        # 神级代码：价格必须在 SMA50 的 0.98 到 1.02 之间 (误差不超过上下2%)
        on_ma50_trampoline = (ma50 * 0.98) <= curr_price <= (ma50 * 1.02)
        dist_ma50_pct = ((curr_price / ma50) - 1) * 100
        ma_status = "✅ 完美踩线" if on_ma50_trampoline else f"❌ 偏离 {dist_ma50_pct:+.1f}%"

        # ==========================================
        # 🎯 毒蛇极致过滤判定 🎯
        # ==========================================
        action = "潜伏观察"
        is_leader = rs_score >= 80
        is_golden_pit = -15 <= dist_high_pct <= -8
        is_sleep_vol = vol_ratio < 0.6

        if is_leader and is_golden_pit:
            if is_sleep_vol and on_ma50_trampoline:
                action = "🐍 毒蛇出洞 (满血击杀)"
            elif is_sleep_vol:
                action = "📉 缩量深蹲 (偏离50日线)"
            elif on_ma50_trampoline:
                action = "⚠️ 踩线放量 (等待缩量)"
            else:
                action = "💦 洗盘未尽 (量大且偏离)"
        elif is_leader and dist_high_pct > -8:
             action = "🔥 跌得不够 (禁追)"
        elif dist_high_pct < -15:
             action = "💀 破位深跌 (跌穿防线)"
        elif not is_leader:
             action = "🗑️ 弱势跟风 (剔除)"
            
        return {
            "rs": rs_score, 
            "act": action, 
            "dist": dist_high_pct,
            "vol_r": vol_ratio, 
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
    trace_id = f"VIPER-X-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🐍 V50.1 缩量毒蛇(极致版) | ID: {trace_id} | {dt_str}")
    tickers =[format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        print(f"⏳ 正在下载数据，锁定主力底牌...")
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
                # 剔除完全不符合底色的辣鸡股，保持表单干净
                if "剔除" in res['act'] or "破位深跌" in res['act']:
                    continue
                    
                results.append([
                    t_raw, 
                    res['act'], 
                    f"{res['dist']:.2f}%",   # 回撤深度 (-15% ~ -8%)
                    f"{res['vol_r']:.2f}x",  # 今日量比 (<0.6)
                    res['ma_stat'],          # 50日均线踩踏情况 (0.98~1.02)
                    res['rs'],               # 强者恒强评级 (>=80)
                    "🎯 满血击杀" if "毒蛇出洞" in res['act'] else "⏳ 尚未达标", 
                    dt_str
                ])
        except Exception as e:
            continue

    # 优先展示出信号的标的，其次按RS强弱排序
    results.sort(key=lambda x: (1 if "🎯" in x[6] else 0, float(x[5])), reverse=True)

    header = [["🐍 V50.1 缩量毒蛇 (大师狙击版)", "ID:", trace_id, "策略:", "专抓50日生命线极度缩量黄金坑", "更新:", dt_str, ""],["代码", "交易指令", "距近期高点(-8%~-15%)", "今日量比(<0.6)", "50日均线蹦床(±2%)", "RS评级(>=80)", "终极判定", "北京时间"]
    ]
    
    try:
        payload = json.loads(json.dumps(header + results, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"🎉 毒蛇信号推送完成，正在同步至表单... HTTP {resp.status_code}")
    except Exception as e:
        print(f"❌ 推送失败: {e}")

if __name__ == "__main__":
    main()
