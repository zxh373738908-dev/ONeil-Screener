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

# 设定基准日期，用于计算 "From XXXX-XX-XX" 字段 (今年以来的涨幅)
BASE_DATE = "2024-12-31" 

SECTOR_MAP = {
    "300502": "半导体", "300308": "半导体", "300394": "半导体", "688313": "半导体", "688041": "半导体", "603501": "半导体",
    "300750": "新能源", "002594": "新能源", "002475": "苹果链", "002371": "特高压",
    "600519": "白酒消费", "000333": "家电消费", "000951": "汽车配件",
    "601899": "有色资源", "601857": "石油石化", "601208": "工业金属", "600105": "永磁资源",
    "600030": "金融证券", "002428": "综合航运", "003031": "智能制造", 
    "601138": "算力/富联", "603259": "医疗/药明", "002222": "猪肉养殖", "603799": "锂电材料"
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
# 2. 全景分析引擎 (共振 + 相对强度 + 市值)
# ==========================================
def analyze_v25(data, bench_series, tickers_raw):
    all_results = []
    
    # 辅助函数：安全计算周期涨幅
    def get_ret(ser, d): 
        if len(ser) < 2: return 0.0
        safe_d = min(len(ser) - 1, d)
        return (ser.iloc[-1] / ser.iloc[-safe_d - 1]) - 1

    # 步骤 A: 计算所有个股今日表现，用于生成行业平均涨幅(共振)
    sector_perf = {}
    for t_raw in tickers_raw:
        try:
            c = data[format_ticker(t_raw)]['Close'].ffill()
            if len(c) >= 2:
                daily_ret = (c.iloc[-1] / c.iloc[-2] - 1) * 100
                s_name = SECTOR_MAP.get(t_raw, "其它")
                sector_perf.setdefault(s_name, []).append(daily_ret)
        except: continue
    
    sector_avg = {k: np.mean(v) for k, v in sector_perf.items()}

    # 步骤 B: 核心指标计算
    for t_raw in tickers_raw:
        t_full = format_ticker(t_raw)
        try:
            df = data[t_full].ffill().dropna()
            if len(df) < 20: continue
            
            c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
            curr_price = float(c.iloc[-1])
            
            # --- 基础与趋势指标 ---
            ret_1d = (c.iloc[-1] / c.iloc[-2] - 1) * 100
            
            ma20 = c.rolling(20).mean().iloc[-1] if len(c) >= 20 else curr_price
            ma60 = c.rolling(60).mean().iloc[-1] if len(c) >= 60 else curr_price
            bias_20 = ((curr_price - ma20) / ma20) * 100
            trend_60 = ((curr_price - ma60) / ma60) * 100
            
            # --- 量价情绪指标 ---
            adr = ((h / l - 1).tail(20).mean()) * 100
            vol_ratio = v.iloc[-1] / v.tail(20).mean() if v.tail(20).mean() != 0 else 1.0
            
            # --- 行业共振指标 ---
            s_name = SECTOR_MAP.get(t_raw, "其它")
            s_avg_ret = sector_avg.get(s_name, 0)
            resonance_str = f"{s_name}({s_avg_ret:+.1f}%)"
            
            # --- 涨幅与相对强度 (R & REL) ---
            r20, r60, r120 = get_ret(c, 20)*100, get_ret(c, 60)*100, get_ret(c, 120)*100
            rel5 = get_ret(c, 5)*100 - get_ret(bench_series, 5)*100
            rel20 = r20 - get_ret(bench_series, 20)*100
            rel60 = r60 - get_ret(bench_series, 60)*100
            rel120 = r120 - get_ret(bench_series, 120)*100
            
            # --- Base Date 涨幅 ---
            try:
                target_dt = pd.to_datetime(BASE_DATE).tz_localize(c.index.tz) if c.index.tz else pd.to_datetime(BASE_DATE)
                past_data = c[c.index <= target_dt]
                ret_base = ((curr_price / past_data.iloc[-1]) - 1) * 100 if not past_data.empty else 0.0
            except: ret_base = 0.0

            # --- 市值获取 (MktCap) ---
            try:
                mcap_raw = yf.Ticker(t_full).fast_info.get('marketCap', 0)
                mcap_str = f"{mcap_raw / 1e9:.1f}B" if mcap_raw > 0 else "N/A"
            except: mcap_str = "N/A"

            # --- 评分模型 ---
            score = rel20 * 0.4 + rel60 * 0.3 + rel120 * 0.3 + 100
            
            all_results.append({
                "Ticker": t_raw, "Industry": s_name, "Price": curr_price,
                "1D": ret_1d, "60D_Trend": trend_60, "Resonance": resonance_str,
                "ADR": adr, "Vol_Ratio": vol_ratio, "Bias": bias_20,
                "MktCap": mcap_str, "Score": score, "S_Avg": s_avg_ret,
                "REL5": rel5, "REL20": rel20, "REL60": rel60, "REL120": rel120,
                "R20": r20, "R60": r60, "R120": r120, "Base_Ret": ret_base
            })
        except Exception as e: continue
    
    return all_results

# ==========================================
# 3. 主流程与数据推流
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"QNT-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 V60.25 全景增强面板启动 | ID: {trace_id}")
    tickers_full = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        # 下载股票和基准数据
        data = yf.download(tickers_full, period="1y", group_by='ticker', progress=False, auto_adjust=True)
        idx = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        bench = idx['Close'].ffill().iloc[:,0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close'].ffill()
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    # 执行分析
    analysis_list = analyze_v25(data, bench, CORE_TICKERS_RAW)

    # 排序与 Rank 分配
    analysis_list.sort(key=lambda x: x['Score'], reverse=True)
    total = len(analysis_list)
    
    rows = []
    for i, x in enumerate(analysis_list):
        # 计算百分位 Rank
        rank = int((total - i) / total * 100) if total > 0 else 0
        
        # Action (交易信号) 判定逻辑
        action = "⚪ 观望"
        if x['S_Avg'] > 1.0 and rank >= 80: action = "🔥 强共振"
        elif x['S_Avg'] > 0.5 and rank >= 50: action = "✅ 联动"
        elif x['Bias'] < -8.0: action = "🟢 超跌"
        elif x['Vol_Ratio'] > 2.0 and x['1D'] > 3.0: action = "⚡ 异动"

        # 格式化组装这精确的 21 列
        rows.append([
            x['Ticker'], 
            x['Industry'], 
            round(x['Score'], 1), 
            f"{x['1D']:+.2f}%", 
            f"{x['60D_Trend']:+.2f}%", 
            action, 
            x['Resonance'], 
            f"{x['ADR']:.2f}%", 
            f"{x['Vol_Ratio']:.2f}", 
            f"{x['Bias']:+.2f}%", 
            x['MktCap'], 
            rank, 
            f"{x['REL5']:+.2f}%", 
            f"{x['REL20']:+.2f}%", 
            f"{x['REL60']:+.2f}%", 
            f"{x['REL120']:+.2f}%", 
            f"{x['R20']:+.2f}%", 
            f"{x['R60']:+.2f}%", 
            f"{x['R120']:+.2f}%", 
            x['Price'], 
            f"{x['Base_Ret']:+.2f}%"
        ])

    # ==========================================
    # 构建表头 (严格 21 列)
    # ==========================================
    # 第一行：面板元数据（使用空字符串补齐至 21 列，避免 Google Sheet 解析错位）
    meta_row = ["📊 V60.25 全景增强面板", "ID:", trace_id, "模式:", "Sector & RPS", "更新:", dt_str] + [""] * 14
    
    # 第二行：正式列名
    col_names = [
        "Ticker", "Industry", "Score", "1D%", "60D Trend", "Action", "Resonance", 
        "ADR", "Vol_Ratio", "Bias", "MktCap", "Rank", "REL5", "REL20", "REL60", 
        "REL120", "R20", "R60", "R120", "Price", f"From {BASE_DATE}"
    ]
    
    payload_data = [meta_row, col_names] + rows

    try:
        payload = json.loads(json.dumps(payload_data, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"📡 结果已推送 | Google 响应: {resp.text}")
    except: 
        print("❌ 推送失败")

if __name__ == "__main__":
    main()
