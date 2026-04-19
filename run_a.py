import yfinance as yf
import pandas as pd
import numpy as np
import datetime, requests, json, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心与行业映射
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyYfpfYNyRhXcyZrfIHEyErECMM82xkCKfZm71RUZ1YL6Xjr5Kca3ruoVJzxcNAwH9q/exec"

# 行业映射表 (根据你提供的 24 只核心标的进行归类)
SECTOR_MAP = {
    "300502": "半导体", "300308": "半导体", "300394": "半导体", "688313": "半导体", "688041": "半导体", "603501": "半导体",
    "300750": "新能源", "002594": "新能源", "002475": "苹果链/电子", "002371": "特高压/电子",
    "600519": "白酒消费", "000333": "家电消费", "000951": "汽车零部件",
    "601899": "有色资源", "601857": "石油石化", "601208": "工业金属", "600105": "永磁/资源",
    "600030": "金融证券", "002428": "综合/航运", "003031": "智能制造", 
    "601138": "算力/工业富联", "603259": "医疗/药明", "002222": "猪肉/养殖", "603799": "锂电/材料"
}

CORE_TICKERS_RAW = list(SECTOR_MAP.keys())

def format_ticker(code):
    c = str(code).zfill(6)
    return f"{c}.SS" if c.startswith('6') else f"{c}.SZ"

def safe_convert(obj):
    if isinstance(obj, (np.integer, np.floating)): 
        return float(obj) if not np.isnan(obj) else 0.0
    return str(obj)

# ==========================================
# 2. V60.25 分析引擎 (板块共振版)
# ==========================================
def analyze_v25(data, bench_series, tickers_raw):
    all_results = []
    
    # 步骤 A: 计算所有个股的今日表现，用于计算行业共振
    sector_perf = {}
    temp_returns = {}
    
    for t_raw in tickers_raw:
        t_full = format_ticker(t_raw)
        try:
            c = data[t_full]['Close'].ffill()
            daily_ret = (c.iloc[-1] / c.iloc[-2] - 1) * 100
            temp_returns[t_raw] = daily_ret
            
            s_name = SECTOR_MAP.get(t_raw, "其它")
            if s_name not in sector_perf: sector_perf[s_name] = []
            sector_perf[s_name].append(daily_ret)
        except: continue

    # 计算行业平均涨幅
    sector_avg = {k: np.mean(v) for k, v in sector_perf.items()}

    # 步骤 B: 详细分析个股指标
    for t_raw in tickers_raw:
        t_full = format_ticker(t_raw)
        try:
            df = data[t_full].ffill().dropna()
            if len(df) < 65: continue
            
            c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
            curr_price = float(c.iloc[-1])
            
            # 1. 基础涨幅
            def get_ret(ser, d): return (ser.iloc[-1] / ser.iloc[-min(len(ser), d+1)]) - 1
            r5, r20, r60 = get_ret(c, 5)*100, get_ret(c, 20)*100, get_ret(c, 60)*100
            
            # 2. 相对强度 (Relative Strength vs Benchmark)
            br20, br60 = get_ret(bench_series, 20)*100, get_ret(bench_series, 60)*100
            rel_20, rel_60 = r20 - br20, r60 - br60

            # 3. 核心技术指标
            adr = ((h / l - 1).tail(20).mean()) * 100
            vol_ratio = v.iloc[-1] / v.tail(20).mean()
            ma20 = c.rolling(20).mean().iloc[-1]
            bias = ((curr_price - ma20) / ma20) * 100
            
            # 4. Resonance (行业共振) 逻辑升级
            s_name = SECTOR_MAP.get(t_raw, "其它")
            s_avg_ret = sector_avg.get(s_name, 0)
            res_val = f"{s_name}({s_avg_ret:+.1f}%)"
            
            # 5. RS_Raw 评分计算 (用于排序)
            rs_score = r20 * 0.4 + r60 * 0.3 + r5 * 0.3 + 100
            
            all_results.append({
                "Code": t_raw, "Price": curr_price, 
                "Resonance": res_val, "ADR": round(adr, 2), 
                "Vol_Ratio": round(vol_ratio, 2), "Bias": round(bias, 2),
                "5D": f"{r5:.2f}%", "20D": f"{r20:.2f}%", "60D": f"{r60:.2f}%",
                "R20": f"{rel_20:.2f}%", "R60": f"{rel_60:.2f}%", 
                "RS_Raw": rs_score, "S_Avg": s_avg_ret
            })
        except: continue
    
    return all_results

# ==========================================
# 3. 主流程
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"RES-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 V60.25 行业共振版启动 | ID: {trace_id}")
    tickers_full = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        # 批量下载
        data = yf.download(tickers_full, period="1y", group_by='ticker', progress=False, auto_adjust=True)
        idx = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        bench = idx['Close'].ffill().iloc[:,0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close'].ffill()
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    # 执行分析
    analysis_list = analyze_v25(data, bench, CORE_TICKERS_RAW)

    # 计算 RS_Rank
    analysis_list.sort(key=lambda x: x['RS_Raw'], reverse=True)
    total = len(analysis_list)
    for i, item in enumerate(analysis_list):
        item['RS_Rank'] = f"{int((total - i) / total * 100)}"

    # 格式化输出表格 (严格对应用户要求的字段)
    header = [
        ["📊 V60.25 行业共振看板", "ID:", trace_id, "模式:", "Sector Momentum", "更新:", dt_str, "", "", "", "", "", ""], 
        ["代码", "Price", "Resonance", "ADR", "Vol_Ratio", "Bias", "RS_Rank", "5D", "20D", "60D", "R20", "R60", "信号"]
    ]
    
    rows = []
    for x in analysis_list:
        # 简单的信号判定
        sig = "⚪ 观望"
        if x['S_Avg'] > 1.0 and float(x['RS_Rank']) > 80: sig = "🔥 强共振"
        elif x['S_Avg'] > 0.5: sig = "✅ 联动"
        elif x['Bias'] < -5: sig = "超跌"

        rows.append([
            x['Code'], x['Price'], x['Resonance'], x['ADR'], x['Vol_Ratio'], 
            x['Bias'], x['RS_Rank'], x['5D'], x['20D'], x['60D'], 
            x['R20'], x['R60'], sig
        ])
    
    try:
        payload = json.loads(json.dumps(header + rows, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"📡 结果已推送 | Google 响应: {resp.text}")
    except: print("❌ 推送失败")

if __name__ == "__main__":
    main()
