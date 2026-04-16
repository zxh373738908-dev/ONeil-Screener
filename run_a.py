import yfinance as yf
import pandas as pd
import numpy as np
import datetime, requests, json, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbzDzZmIHMbn8JZH0Yw-zww7Jh7C9HvqVJhefdeKIRKAmwd1t6MR3XHSg9YWVPB3gXiM/exec" # 请填入你上一步生成的URL

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
# 2. 深度逻辑分析引擎
# ==========================================
def analyze_stock_pro(df_s, bench_series, t_code):
    try:
        # 数据清洗
        c = df_s['Close'].ffill().dropna()
        if isinstance(c, pd.DataFrame): c = c.iloc[:, 0]
        h = df_s['High'].ffill().dropna()
        if isinstance(h, pd.DataFrame): h = h.iloc[:, 0]
        l = df_s['Low'].ffill().dropna()
        if isinstance(l, pd.DataFrame): l = l.iloc[:, 0]
        v = df_s['Volume'].ffill().dropna()
        if isinstance(v, pd.DataFrame): v = v.iloc[:, 0]

        if len(c) < 250: return None
        curr, prev = float(c.iloc[-1]), float(c.iloc[-2])
        
        # 1. 增强型 RS 评分 (250日、60日、20日加权)
        def get_ret(ser, days): return (ser.iloc[-1] / ser.iloc[-min(len(ser), days)]) - 1
        s_ret = get_ret(c, 250)*0.3 + get_ret(c, 60)*0.4 + get_ret(c, 20)*0.3
        b_ret = get_ret(bench_series, 250)*0.3 + get_ret(bench_series, 60)*0.4 + get_ret(bench_series, 20)*0.3
        rs_score = round((s_ret - b_ret + 1) * 85, 2)

        # 2. 趋势斜率 (判定均线是否向上)
        ma50 = c.rolling(50).mean()
        m50_c = float(ma50.iloc[-1])
        m50_p = float(ma50.iloc[-6]) # 5天前
        slope = (m50_c - m50_p) / m50_p * 100
        is_trend_up = slope > 0.05 

        # 3. 核心指标
        vol_avg = v.iloc[-21:-1].mean()
        v_r = float(v.iloc[-1] / vol_avg) if vol_avg > 0 else 1.0
        dist = float(((curr / h.tail(22).max()) - 1) * 100)
        amp = float((float(h.iloc[-1]) - float(l.iloc[-1])) / prev * 100)

        # 4. 极致判定逻辑
        level, act, win, guide = "⚪ 观察", "潜伏观察", "40%", "等待回踩"
        near_ma50 = (m50_c * 0.98) <= curr <= (m50_c * 1.02) # 缩窄至2%误差
        
        if rs_score >= 80:
            if near_ma50:
                if is_trend_up and v_r < 0.65:
                    level, act, win, guide = "🐍 满血", "毒蛇出洞", "90%", "多头缩量踩线,极品"
                elif not is_trend_up and v_r < 0.65:
                    level, act, win, guide = "⚠️ 警惕", "弱势踩线", "55%", "均线走平,谨防破位"
                else:
                    level, act, win, guide = "⏳ 观察", "踩线待缩", "65%", "等待量比降至0.6以下"
            elif dist > -6:
                level, act, win, guide = "🔥 禁追", "强势整理", "60%", "位置太高,不接力"
            else:
                level, act, win, guide = "🟡 准备", "深蹲埋伏", "75%", "RS强+深度回调,待踩线"
        elif curr < m50_c * 0.96:
            level, act, win, guide = "💀 破位", "速离放弃", "10%", "趋势已坏,坚决止损"

        # 5. 返回格式化数据 (RS分转为带小数的字符串，防止日期错误)
        return [
            t_code, 
            act, 
            level, 
            f"{dist:.2f}%", 
            f"{v_r:.2f}x", 
            f"{amp:.2f}%", 
            "📈 向上" if is_trend_up else "📉 向下/平", 
            win, 
            guide, 
            f"{rs_score:.2f}" # 强制保留两位小数，Google不会识别成日期
        ]
    except: return None

# ==========================================
# 3. 主程序
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"VP-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 V60.11 极致进化版 | ID: {trace_id}")
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        idx = yf.download("000300.SS", period="2y", progress=False, auto_adjust=True)
        bench = idx['Close']
        if isinstance(bench, pd.DataFrame): bench = bench.iloc[:, 0]
        all_data = yf.download(tickers, period="2y", group_by='ticker', progress=False, auto_adjust=True)
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    results = []
    for t_full in tickers:
        t_raw = t_full.split('.')[0]
        try:
            res = analyze_stock_pro(all_data[t_full], bench, t_raw)
            if res: 
                results.append(res)
                print(f"✅ {t_raw} 分析完成")
        except: continue

    # 排序：按照胜率和RS分排序
    results.sort(key=lambda x: (x[7], x[9]), reverse=True)
    
    header = [
        ["🚀 V60.11 毒蛇极致版", "ID:", trace_id, "策略:", "向上生命线+地量窄幅", "更新:", dt_str, "", "", ""], 
        ["代码", "指令", "等级", "距高点", "量比", "振幅", "MA50斜率", "胜率预测", "实战指引", "RS强弱评分"]
    ]
    
    if results:
        try:
            payload = json.loads(json.dumps(header + results, default=safe_convert))
            resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
            print(f"📡 Google 响应: {resp.text}")
        except Exception as e: print(f"❌ 推送失败: {e}")

if __name__ == "__main__":
    main()
