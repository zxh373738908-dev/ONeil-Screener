import numpy as np
import pandas as pd

def analyze_stock(df, benchmark_series, symbol="Unknown", ACCOUNT_SIZE=100000, MAX_RISK_PER_TRADE=0.01):
    """
    量化选股核心引擎 - (VCP + 欧奈尔 + 口袋枢轴 + 筹码POC)
    """
    # 【优化1：数据有效性拦截】数据太少（如刚上市几天）直接跳过，防止数组越界
    if df is None or len(df) < 60:
        return None
        
    try:
        # 获取底层 numpy 数组提速
        close = df['Close'].values.astype(float)
        open_p = df['Open'].values.astype(float)
        high = df['High'].values.astype(float)
        low = df['Low'].values.astype(float)
        vol = df['Volume'].values.astype(float)
        cp = close[-1]
        
        # 针对次新股或停牌股的长度自适应 (不足252天的取当前最大长度)
        L = len(close)
        len200 = min(L, 200)
        len252 = min(L, 252)
        len126 = min(L, 126)
        
        # 1. 趋势模板与生命线计算
        ma10 = np.mean(close[-min(L, 10):])
        ma20 = np.mean(close[-min(L, 20):])
        ma50 = np.mean(close[-min(L, 50):])
        ma200 = np.mean(close[-len200:])
        
        # Stage 2 阶段判定 (放宽条件，允许长期横盘后刚站上年线)
        is_stage_2 = (cp > ma50) and (cp > ma200) and (ma50 > ma200 * 0.95)
        is_bottom_reversal = (cp > ma50) and (ma200 > cp) and (cp > np.min(close[-min(L, 60):]) * 1.15)

        # 主升浪特征
        is_main_uptrend = (
            (cp > ma10) and (cp > ma20) and 
            (ma10 > ma50) and (ma20 > ma50) and 
            (ma20 > np.mean(close[-min(L, 25):-5])) and 
            (cp >= np.max(close[-min(L, 60):]) * 0.85)
        )

        # 2. RS 相对强度测算 (引入基准指数对比)
        # 【优化2：处理基准缺失或不对齐导致的 NaN 问题】
        bench_val = benchmark_series.reindex(df.index).ffill().bfill().values
        # 【优化3：安全除法，防止 bench_val 为 0】
        rs_line = np.nan_to_num(close / np.maximum(bench_val, 1e-5), nan=1.0)
        
        # 10日前RS对比（注意长度安全）
        idx_10 = -min(L, 10)
        rs_velocity = (rs_line[-1] - rs_line[idx_10]) / np.maximum(rs_line[idx_10], 1e-5) * 100
        rs_nh = rs_line[-1] >= np.max(rs_line[-len126:]) 

        # 3. VCP 紧致度计算
        recent_10_closes = close[-min(L, 10):]
        # 使用最大值限制分母，防止均值为0
        tightness = (np.std(recent_10_closes) / np.maximum(np.mean(recent_10_closes), 1e-5)) * 100
        
        # 4. 机构能量 (量能潮)
        avg_vol20 = np.mean(vol[-min(L, 20):])
        vol_surge = vol[-1] / np.maximum(avg_vol20, 1e-5)
        vdu = vol[-1] < avg_vol20 * 0.65 

        # ---- 【🌟 参数中心：便于后续修改与回测】 ----
        THR_TIGHT = 4.5        # 紧致度极限 (A股创业板可放宽至 5.0)
        THR_SURGE_BRK = 1.5    # 突破要求爆量倍数
        THR_SURGE_REV = 1.8    # 底部反转要求爆量倍数
        THR_BIAS_MA10 = 15     # 短线乖离率警戒线
        THR_POC_DIST = 25      # 筹码偏离警戒线

        # 5. 综合战法判定树
        action = "观察"
        prio = 50
        
        if rs_nh and cp < np.max(close[-min(L, 20):]) * 1.05 and tightness < THR_TIGHT:
            action, prio = "👁️ 奇點先行(Stealth)", 95
        elif is_stage_2 and vdu and tightness < THR_TIGHT:
            action, prio = "🐉 老龍回頭(V-Dry)", 90
        elif rs_nh and cp >= np.max(close[-len126:]) and vol_surge > THR_SURGE_BRK:
            action, prio = "🚀 巔峰突破(Breakout)", 92
        elif is_stage_2 and rs_nh and rs_velocity > 1.0:
            action, prio = "💎 雙重共振(Leader)", 88
        elif is_bottom_reversal and vol_surge > THR_SURGE_REV:
            action, prio = "🌋 困境起爆(Reversal)", 85

        # 主升浪叠加 Buff
        if is_main_uptrend:
            if action == "观察":
                action, prio = "🚀 主升浪(Uptrend)", 94
            else:
                action = action.replace(")", " + 🚀主升浪)")
                prio += 10

        # 6. 【补丁4：筹码峰 (POC) 计算】
        hist_close = close[-len126:]
        hist_vol = vol[-len126:]
        hist_min, hist_max = np.min(hist_close), np.max(hist_close)
        
        if hist_max > hist_min:
            bins = np.linspace(hist_min, hist_max, 50)
            indices = np.clip(np.digitize(hist_close, bins) - 1, 0, 49)
            vol_bins = np.zeros(50)
            np.add.at(vol_bins, indices, hist_vol)
            poc_idx = np.argmax(vol_bins)
            poc_price = bins[poc_idx]
        else:
            poc_price = hist_close[-1]

        dist_poc = ((cp - poc_price) / np.maximum(poc_price, 1e-5)) * 100
        bias_ma10 = (cp - ma10) / np.maximum(ma10, 1e-5) * 100

        # 一票否决权：高空缺氧绝对禁买
        if action != "观察" and (bias_ma10 > THR_BIAS_MA10 and dist_poc > THR_POC_DIST):
            action = "☠️ 极度延伸(禁买)"
            prio = 10 

        # 7. 【补丁5：口袋枢轴 (Pocket Pivot)】
        # 【优化4：全面向量化替代 for 循环，提升计算速度】
        lookback_days = min(L, 20)
        avg_vol20_series = pd.Series(vol).rolling(window=20, min_periods=1).mean().values
        
        recent_vol = vol[-3:]
        recent_avg = avg_vol20_series[-3:]
        recent_close = close[-3:]
        recent_open = open_p[-3:]
        
        # 向量化判断：近三天是否存在任意一天放量1.3倍且收阳线
        pocket_pivot = np.any((recent_vol > recent_avg * 1.3) & (recent_close > recent_open))

        # 8. 多重结构止损计算
        adr_days = min(L, 20)
        adr_20 = np.mean((high[-adr_days:] - low[-adr_days:]) / np.maximum(close[-adr_days:], 1e-5)) * 100
        adr_stop = cp * (1 - adr_20 * 0.01 * 1.5)
        
        if "主升浪" in action and "禁买" not in action:
            struct_stop = ma20 * 0.98 
        else:
            struct_stop = ma50 * 0.98
            
        final_stop = max(adr_stop, struct_stop) 

        # 9. 建议仓位管理 (Kelly Criterion 简化版)
        risk_per_share = cp - final_stop
        suggested_shares = 0
        if risk_per_share > 0:
            suggested_shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // risk_per_share

        # 构造原始相对强度 raw_rs (用 max 防止越界)
        rs_raw_val = (
            cp / np.maximum(close[-min(L, 63)], 1e-5) * 2 + 
            cp / np.maximum(close[-min(L, 126)], 1e-5) + 
            cp / np.maximum(close[-len252], 1e-5)
        )

        return {
            "Symbol": symbol,              # 返回股票代码，便于后续 DataFrame 合并
            "Action": action, 
            "Score": prio,                 # 更名为 Score 更直观
            "Price": round(cp, 2), 
            "Tight%": round(tightness, 2), 
            "Vol_Ratio": round(vol_surge, 2), 
            "ADR%": round(adr_20, 2), 
            "Stop_Price": round(final_stop, 2),
            "Shares": int(suggested_shares), 
            "RS_Vel": round(rs_velocity, 2),
            "Dist_POC%": round(dist_poc, 2),
            "PocketPivot": "🔥 是" if pocket_pivot else "否",
            "Is_Bull": bool(cp > ma200),   # 转换为标准 bool
            "RS_Raw": round(rs_raw_val, 2)
        }
        
    except Exception as e:
        # 【优化5：精细化报错输出，方便定位是哪只股票卡住了代码】
        print(f"❌ [Error] 分析股票 {symbol} 时发生异常: {str(e)}")
        return None
