import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings

# 屏蔽不必要的警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (A股定制版)
# ==========================================
# 您的 Google WebApp URL
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyaG1UpjC3NLqrqC5T3oIcGM8mnstV-AzlmEDTMdrcfgsOzjzek3aeAqYtg-74ZHv8_/exec"

# A股核心观察池 (涵盖白酒、半导体、新能源、高股息、AI算力等)
CORE_TICKERS_RAW = [
    "600519", "300750", "601318", "000858", "600036", # 茅台, 宁德, 平安, 五粮液, 招行
    "601138", "300274", "603501", "688041", "600900", # 工业富联, 阳光电源, 韦尔, 海光, 长江电力
    "002594", "601899", "002415", "000333", "600030", # 比亚迪, 紫金矿业, 海康威视, 美的, 中信证券
    "000063", "300502", "002475", "600150", "688981"  # 中兴通讯, 新易盛, 立讯精密, 中国船舶, 中芯国际
]

SECTOR_MAP = {
    "600519": "大消费/白酒", "000858": "大消费/白酒", "000333": "大消费/家电",
    "300750": "新能源/电池", "300274": "新能源/光伏", "002594": "新能源/汽车",
    "603501": "半导体/IC", "688041": "半导体/算力", "688981": "半导体/代工",
    "601138": "AI/硬件驱动", "300502": "AI/光模块", "000063": "AI/通信设备",
    "601318": "大金融/保险", "600036": "大金融/银行", "600030": "大金融/券商",
    "600900": "红利/长江电力", "601899": "资源/紫金矿业", "600150": "中特估/船舶"
}

def format_ticker(code):
    """自动匹配 A股 交易所后缀"""
    if code.startswith('6'): return f"{code}.SS"
    if code.startswith('0') or code.startswith('3'): return f"{code}.SZ"
    if code.startswith('8') or code.startswith('4'): return f"{code}.BJ"
    return code

# ==========================================
# 2. 深度数据净化逻辑
# ==========================================
def safe_convert(obj):
    """将 NumPy/Pandas 类型强制转为 Python 基础类型，防止 JSON 报错"""
    if isinstance(obj, (np.integer, np.floating)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    return str(obj)

# ==========================================
# 3. 核心算法逻辑 (VCP 紧致度 + RS 相对强度)
# ==========================================
def calculate_v1000_nexus_ashare(df, bench_series):
    try:
        if len(df) < 60: return None
        
        # 提取收盘价、最高、最低，并确保是 Series
        close = df['Close'].squeeze()
        high = df['High'].squeeze()
        low = df['Low'].squeeze()
        curr_price = float(close.iloc[-1])
        
        # 1. 相对强度 (RS) - 对标沪深300
        # 对齐索引
        bench_aligned = bench_series.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        rs_nh_20 = bool(rs_line.iloc[-1] >= rs_line.tail(25).max())
        
        # 2. 紧致度 (VCP) - 过去12天波动率缩减
        tightness = float((close.tail(12).std() / close.tail(12).mean()) * 100)
        
        # 3. RS 表现分 (1个月、3个月、半年加权)
        def get_perf(days):
            if len(close) < days: return 0.0
            return (curr_price - close.iloc[-days]) / close.iloc[-days]
        
        rs_score = float(get_perf(21)*4 + get_perf(63)*2 + get_perf(126))

        signals, base_res = [], 0
        
        # 信号 A：奇点突破 (RS 新高 + 价格波动极小)
        if rs_nh_20 and tightness < 2.5: 
            signals.append("👁️奇点"); base_res += 4
        # 信号 B：高位平台 (距离半年高点 8% 以内)
        h120 = float(high.tail(120).max())
        if curr_price >= h120 * 0.92: 
            signals.append("🚀高位"); base_res += 2
        
        # 信号 C：多头排列 (站上20日均线)
        ma20 = close.rolling(20).mean().iloc[-1]
        if curr_price > ma20:
            base_res += 1
        
        # 振幅 ADR
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr
        }
    except:
        return None

# ==========================================
# 4. 主指挥引擎
# ==========================================
def run_v1000_ashare():
    start_time = time.time()
    print("🚀 V1000 [A股枢纽版] 启动分析...")

    # 格式化所有代码
    tickers = [format_ticker(t) for t in CORE_TICKERS_RAW]

    try:
        # 下载数据
        data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
        # 基准数据 (沪深300)
        m_idx = yf.download("000300.SS", period="1y", progress=False)
        bench_series = m_idx['Close'].squeeze()
        
        # 大盘当日涨跌
        mkt_change = ((bench_series.iloc[-1] - bench_series.iloc[-2]) / bench_series.iloc[-2]) * 100
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    candidates = []
    sector_cluster = {}
    
    for t_full in tickers:
        try:
            t_raw = t_full.split('.')[0]
            # yfinance 下载多股时，data 是 MultiIndex
            df_t = data[t_full].dropna()
            if df_t.empty: continue
            
            res = calculate_v1000_nexus_ashare(df_t, bench_series)
            if res:
                res["Ticker"] = t_raw
                res["Sector"] = SECTOR_MAP.get(t_raw, "其他板块")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 暂无枢纽信号"); return

    # 排序逻辑：Base_Res (信号数量) > RS_Score (强度)
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(15)
    
    final_list = []
    for _, row in sorted_df.iterrows():
        rating = "💎SSS 领涨" if row['Base_Res'] >= 5 else ("🔥强势" if row['RS_Score'] > 0.15 else "✅观察")
        sig_str = " + ".join(row['Signals']) if row['Signals'] else "📈 稳健趋势"
        cluster = f"{sector_cluster.get(row['Sector'], 1)}家共振"
        
        final_list.append([
            str(row['Ticker']),
            rating,
            sig_str,
            cluster,
            f"{round(row['ADR'], 2)}%", 
            round(float(row['Price']), 2),
            f"{round(float(row['Tightness']), 2)}%",
            round(float(row['RS_Score']), 2),
            str(row['Sector']),
            "A-Share"
        ])

    # 时间与状态头
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%m-%d %H:%M')
    mkt_status = "多头环境" if mkt_change > 0 else "缩量回调"
    
    header = [
        ["🏰 V50-Guardian A股枢纽", "更新:", bj_now, "大盘涨跌:", f"{round(mkt_change,2)}%", mkt_status, "", "", "", ""],
        ["代码", "综合评级", "核心信号", "板块共振", "波动率AD", "现价", "收盘紧致度", "RS强度强度", "所属行业", "市场"]
    ]
    
    matrix = header + final_list
    
    # 5. 推送至 Google Sheets
    try:
        # 使用 json.dumps 的 default 参数处理所有非标数据
        clean_json = json.loads(json.dumps(matrix, default=safe_convert))
        
        resp = requests.post(WEBAPP_URL, json=clean_json, timeout=15)
        
        print(f"📡 服务器响应: {resp.text}")
        print(f"🎉 A股数据同步成功！耗时: {round(time.time() - start_time, 2)}s")
        
    except Exception as e:
        print(f"❌ 同步过程中出错: {e}")

if __name__ == "__main__":
    run_v1000_ashare()
