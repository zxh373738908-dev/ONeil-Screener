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

# 核心股票池
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
# 2. V60.2 优化版逻辑引擎
# ==========================================
def analyze_stock_viper_v2(df_in, bench_ser, t_name):
    try:
        close = get_safe_col(df_in, 'Close').astype(float)
        high = get_safe_col(df_in, 'High').astype(float)
        low = get_safe_col(df_in, 'Low').astype(float)
        vol = get_safe_col(df_in, 'Volume').astype(float)
        
        if close.empty or len(close) < 250: return None
        
        curr_price = float(close.iloc[-1])
        prev_close = float(close.iloc[-2])
        
        # --- 1. 均线状态与斜率 (趋势强度) ---
        ma50_series = close.rolling(50).mean()
        ma50_curr = ma50_series.iloc[-1]
        # 计算过去5天的斜率 (百分比变化)
        ma50_slope = (ma50_curr - ma50_series.iloc[-6]) / ma50_series.iloc[-6] * 100
        
        # --- 2. RS 强者恒强评级 ---
        def get_p(s, d): return float((s.iloc[-1] / s.iloc[-d]) - 1)
        s_p = get_p(close, 250)*0.4 + get_p(close, 60)*0.3 + get_p(close, 20)*0.3
        b_p = get_p(bench_ser, 250)*0.4 + get_p(bench_ser, 60)*0.3 + get_p(bench_ser, 20)*0.3
        rs_score = round((s_p - b_p + 1) * 85, 2)
        
        # --- 3. 波动率压缩 (振幅) ---
        amplitude = (high.iloc[-1] - low.iloc[-1]) / prev_close * 100
        
        # --- 4. 核心指标 ---
        recent_high = float(high.tail(22).max())
        dist_high_pct = ((curr_price / recent_high) - 1) * 100 
        avg_vol_20 = vol.iloc[-21:-1].mean()
        vol_ratio = float(vol.iloc[-1] / avg_vol_20) if avg_vol_20 > 0 else 1.0
        
        # ==========================================
        # 🎯 进攻与防守判定逻辑
        # ==========================================
        # 核心参数
        is_leader = rs_score >= 80
        is_at_ma50 = (ma50_curr * 0.98) <= curr_price <= (ma50_curr * 1.02)
        is_ma50_up = ma50_slope > 0.05 # 均线斜率微幅向上
        is_quiet = vol_ratio < 0.65 and amplitude < 3.0 # 地量且极窄振幅
        
        action = "潜伏观察"
        signal_level = "⚪ 待机"
        defense_guide = "保持关注"
        win_rate = "50%"

        # 进攻模式
        if is_leader and is_at_ma50:
            if is_ma50_up and is_quiet:
                action = "🔥 进攻 (毒蛇狙击点)"
                signal_level = "🚀 强烈进攻"
                defense_guide = "以今日最低价止损"
                win_rate = "85%"
            elif is_ma50_up:
                action = "🎯 准备 (等待极度缩量)"
                signal_level = "🟡 准备阶段"
                defense_guide = "不破50日线不入"
                win_rate = "65%"
            else:
                action = "📉 踩线 (均线走平趋势弱)"
                signal_level = "🔵 低位震荡"
                defense_guide = "轻仓试探"
                win_rate = "55%"
        
        # 防守模式
        if curr_price < ma50_curr * 0.97:
            action = "💀 防守 (跌穿生命线)"
            signal_level = "🚫 撤退"
            defense_guide = "立即止损离场"
            win_rate = "10%"
        elif vol_ratio > 1.8 and curr_price < prev_close:
            action = "⚠️ 预警 (放量下跌)"
            signal_level = "🔴 风险预警"
            defense_guide = "减仓观望"
            win_rate = "30%"
        elif dist_high_pct > -7:
            action = "🔥 禁追 (高位整理)"
            signal_level = "⏳ 等待回撤"
            defense_guide = "切勿追高"

        return {
            "rs": rs_score, 
            "act": action, 
            "dist": f"{dist_high_pct:.1f}%",
            "vol_r": f"{vol_ratio:.2f}x", 
            "ma_slope": "📈 向上" if is_ma50_up else "📉 走平/向下",
            "amp": f"{amplitude:.1f}%",
            "level": signal_level,
            "defense": defense_guide,
            "win": win_rate
        }
    except Exception as e:
        return None

# ==========================================
# 3. 主流程
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"VIPER-PRO-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 V60.2 终极毒蛇狙击 | ID: {trace_id} | {dt_str}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        data = yf.download(tickers, period="2y", group_by='ticker', progress=False, auto_adjust=True)
        idx_df = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench_close = get_safe_col(idx_df, 'Close')
    except Exception as e:
        print(f"❌ 数据获取错误: {e}"); return

    results = []
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            df_s = data[t_full] if len(tickers) > 1 else data
            res = analyze_stock_viper_v2(df_s, bench_close, t_raw)
            if res:
                # 过滤掉完全没戏的弱势股
                if res['rs'] < 60: continue
                
                results.append([
                    t_raw, 
                    res['act'], 
                    res['level'],      # 进攻/防守等级
                    res['dist'],       # 回撤深度
                    res['vol_r'],      # 量比
                    res['amp'],        # 振幅 (波动挤压)
                    res['ma_slope'],   # 均线斜率
                    res['win'],        # 预测胜率
                    res['defense'],    # 实战指引
                    res['rs']          # RS分数
                ])
        except: continue

    # 排序：按进攻等级和胜率排序
    results.sort(key=lambda x: (x[2], x[9]), reverse=True)

    header = [
        ["🚀 V60.2 终极毒蛇 (进攻版)", "ID:", trace_id, "策略:", "50日线+波动挤压+斜率过滤", "更新:", dt_str, ""],
        ["代码", "交易指令", "信号等级", "距高点", "量比", "振幅(挤压)", "MA50趋势", "胜率预测", "实战指引", "RS分"]
    ]
    
    try:
        payload = json.loads(json.dumps(header + results, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"🎉 信号同步完成! 状态: {resp.status_code}")
    except Exception as e:
        print(f"❌ 推送失败: {e}")

if __name__ == "__main__":
    main()
