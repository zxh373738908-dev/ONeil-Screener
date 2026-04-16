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
# 2. V60.3 优化版逻辑引擎 (动态兼容版)
# ==========================================
def analyze_stock_viper_v3(df_in, bench_ser, t_name):
    try:
        close = get_safe_col(df_in, 'Close').astype(float)
        high = get_safe_col(df_in, 'High').astype(float)
        low = get_safe_col(df_in, 'Low').astype(float)
        vol = get_safe_col(df_in, 'Volume').astype(float)
        
        if close.empty or len(close) < 120: return None
        
        curr_price = float(close.iloc[-1])
        prev_close = float(close.iloc[-2])
        
        # --- 1. 均线状态与斜率 ---
        ma50_series = close.rolling(50).mean()
        ma50_curr = ma50_series.iloc[-1]
        ma50_slope = (ma50_curr - ma50_series.iloc[-5]) / ma50_series.iloc[-5] * 100
        
        # --- 2. RS 评级 ---
        def get_p(s, d): return float((s.iloc[-1] / s.iloc[-d]) - 1)
        s_p = get_p(close, 120)*0.5 + get_p(close, 20)*0.5 # 缩短周期增加敏感度
        b_p = get_p(bench_ser, 120)*0.5 + get_p(bench_ser, 20)*0.5
        rs_score = round((s_p - b_p + 1) * 85, 2)
        
        # --- 3. 核心指标 ---
        amplitude = (high.iloc[-1] - low.iloc[-1]) / prev_close * 100
        recent_high = float(high.tail(22).max())
        dist_high_pct = ((curr_price / recent_high) - 1) * 100 
        avg_vol_20 = vol.iloc[-21:-1].mean()
        vol_ratio = float(vol.iloc[-1] / avg_vol_20) if avg_vol_20 > 0 else 1.0
        
        # ==========================================
        # 🎯 进攻与防守判定 (放宽条件)
        # ==========================================
        is_leader = rs_score >= 75 # 从80放宽到75
        is_near_ma50 = (ma50_curr * 0.97) <= curr_price <= (ma50_curr * 1.03) # 放宽到3%误差
        is_ma50_not_down = ma50_slope > -0.1 # 只要不是明显下跌
        
        status = "潜伏观察"
        level = "⚪ 待机"
        guide = "耐心等待信号"
        win = "50%"

        if is_leader:
            if is_near_ma50:
                if vol_ratio < 0.7 and amplitude < 3.5:
                    status = "🐍 毒蛇出洞 (进攻)"
                    level = "🚀 强烈进攻"
                    guide = "绝佳缩量位，博弈反弹"
                    win = "85%"
                elif vol_ratio < 1.0:
                    status = "🎯 准备 (临界缩量)"
                    level = "🟡 重点关注"
                    guide = "即将缩至极致，明日关注"
                    win = "70%"
                else:
                    status = "💦 踩线放量 (待缩量)"
                    level = "🔵 观察支撑"
                    guide = "50日线有支撑但量稍大"
                    win = "55%"
            elif dist_high_pct > -6:
                status = "🔥 强势整理 (禁追)"
                level = "⏳ 还没跌到位"
                guide = "等回踩50日线再看"
        
        if curr_price < ma50_curr * 0.96:
            status = "💀 破位 (放弃)"
            level = "🚫 撤退"
            guide = "趋势走坏，换股"
            win = "10%"

        return {
            "rs": rs_score, "act": status, "dist": f"{dist_high_pct:.1f}%",
            "vol_r": f"{vol_ratio:.2f}x", "ma_slope": "📈 向上" if ma50_slope > 0 else "📉 向下",
            "amp": f"{amplitude:.1f}%", "level": level, "defense": guide, "win": win
        }
    except: return None

# ==========================================
# 3. 主流程
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"VIPER-V3-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 V60.3 动态毒蛇版 | ID: {trace_id} | {dt_str}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        data = yf.download(tickers, period="1y", group_by='ticker', progress=False, auto_adjust=True)
        idx_df = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        bench_close = get_safe_col(idx_df, 'Close')
        print(f"📊 已下载 {len(tickers)} 只标的数据，开始扫描...")
    except Exception as e:
        print(f"❌ 数据获取错误: {e}"); return

    results = []
    for t_full in tickers:
        t_raw = t_full.split('.')[0]
        df_s = data[t_full] if len(tickers) > 1 else data
        res = analyze_stock_viper_v3(df_s, bench_close, t_raw)
        if res:
            # 只要不是完全弱势或破位的，都展示出来
            if "放弃" in res['act']: continue
            results.append([
                t_raw, res['act'], res['level'], res['dist'], res['vol_r'],
                res['amp'], res['ma_slope'], res['win'], res['defense'], res['rs']
            ])

    # 排序：进攻等级优先，其次RS分数
    results.sort(key=lambda x: (x[2], x[9]), reverse=True)

    header = [
        ["🚀 V60.3 毒蛇 (全量扫描版)", "ID:", trace_id, "策略:", "放宽过滤条件以捕获潜在机会", "更新:", dt_str, ""],
        ["代码", "交易指令", "信号等级", "距高点", "量比", "振幅", "MA50趋势", "预测胜率", "实战指引", "RS分"]
    ]
    
    try:
        payload = json.loads(json.dumps(header + results, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"✅ 扫描完成！共选出 {len(results)} 只标的，已推送到表单。")
    except Exception as e:
        print(f"❌ 推送失败: {e}")

if __name__ == "__main__":
    main()
