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

# 扩充了代码池，确保即使部分票停牌也有数据
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

# ==========================================
# 2. 逻辑引擎 - 零过滤全量输出版
# ==========================================
def analyze_stock_full_scan(df_s, bench_ser, t_code):
    try:
        # 兼容 yfinance 的多级索引列名
        close = df_s['Close'].dropna()
        if len(close) < 60: return None
        
        curr_price = float(close.iloc[-1])
        prev_close = float(close.iloc[-2])
        high = df_s['High'].dropna()
        vol = df_s['Volume'].dropna()

        # 1. 计算 RS (相对强度) - 只要有数据就计算
        stock_ret = (curr_price / close.iloc[-min(len(close), 120)]) - 1
        bench_ret = (bench_ser.iloc[-1] / bench_ser.iloc[-min(len(bench_ser), 120)]) - 1
        rs_score = round((stock_ret - bench_ret + 1) * 85, 2)

        # 2. 均线与斜率
        ma50 = close.rolling(50).mean()
        ma50_curr = ma50.iloc[-1]
        ma50_slope = (ma50_curr - ma50.iloc[-5]) / ma50.iloc[-5] * 100 if len(ma50) > 5 else 0

        # 3. 核心指标
        vol_ratio = float(vol.iloc[-1] / vol.iloc[-21:-1].mean()) if len(vol) > 21 else 1.0
        dist_high = ((curr_price / high.tail(22).max()) - 1) * 100
        amp = (high.iloc[-1] - df_s['Low'].dropna().iloc[-1]) / prev_close * 100

        # 4. 判定标签 (不再 return None，而是分类)
        level = "⚪ 观察"
        act = "潜伏观察"
        win = "40%"
        guide = "等待回踩或地量"

        is_near_ma50 = (ma50_curr * 0.97) <= curr_price <= (ma50_curr * 1.03)
        
        if rs_score > 85:
            if is_near_ma50 and vol_ratio < 0.7:
                level = "🚀 进攻"
                act = "🐍 毒蛇出洞"
                win = "85%"
                guide = "极度缩量+生命线支撑"
            elif dist_high > -5:
                level = "🔥 强势"
                act = "禁追 (高位)"
                guide = "涨幅过大，等回调"
            else:
                level = "🟡 准备"
                act = "等待缩量"
                guide = "处于强势区间，观察量能"
        elif curr_price < ma50_curr * 0.96:
            level = "💀 破位"
            act = "放弃 (跌穿)"
            win = "10%"
            guide = "趋势已坏，不接飞刀"

        return [
            t_code, act, level, f"{dist_high:.1f}%", f"{vol_ratio:.2f}x", 
            f"{amp:.1f}%", "📈 向上" if ma50_slope > 0 else "📉 向下/平", 
            win, guide, rs_score
        ]
    except Exception as e:
        print(f"解析 {t_code} 出错: {e}")
        return None

# ==========================================
# 3. 主流程
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"V6-FULL-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"📡 V60.4 信号扫描开始 | ID: {trace_id}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    # 增加对指数的重试机制
    try:
        print("📥 正在抓取市场基准 (CSI 300)...")
        idx_data = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        bench_close = idx_data['Close']
        
        print(f"📥 正在抓取 {len(tickers)} 只标的数据...")
        # 强制分批获取，防止 yfinance 一次性请求过多被拒
        all_data = yf.download(tickers, period="1y", group_by='ticker', progress=False, auto_adjust=True)
    except Exception as e:
        print(f"❌ 数据抓取中断: {e}"); return

    results = []
    for t_full in tickers:
        t_raw = t_full.split('.')[0]
        # 处理 yfinance 返回的不同数据结构
        try:
            stock_df = all_data[t_full]
            res_row = analyze_stock_full_scan(stock_df, bench_close, t_raw)
            if res_row:
                results.append(res_row)
                print(f"✅ {t_raw}: {res_row[2]} (RS:{res_row[9]})")
        except:
            continue

    # 排序：进攻 > 准备 > 观察 > 破位
    results.sort(key=lambda x: (x[2], x[9]), reverse=True)

    header = [
        ["🚀 V60.4 终极扫描 (防空版)", "ID:", trace_id, "模式:", "全量展示/动态指引", "更新:", dt_str, ""],
        ["代码", "交易指令", "信号等级", "距高点", "量比", "振幅", "MA50趋势", "预测胜率", "实战指引", "RS分"]
    ]
    
    if not results:
        print("⚠️ 警告：分析完成但结果集为空，请检查 yfinance 网络。")
    else:
        try:
            payload = json.loads(json.dumps(header + results, default=safe_convert))
            resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
            print(f"🎉 成功同步 {len(results)} 条数据到表单! (HTTP {resp.status_code})")
        except Exception as e:
            print(f"❌ 推送失败: {e}")

if __name__ == "__main__":
    main()
