import yfinance as yf
import pandas as pd
import numpy as np
import datetime, time, requests, json, math, warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (A股定制版)
# ==========================================
# 填入您的 Google WebApp URL
WEBAPP_URL = "https://script.google.com/macros/s/您的_ID/exec"

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
    "601138": "AI/硬件", "300502": "AI/光模块", "000063": "AI/通信",
    "601318": "大金融/保险", "600036": "大金融/银行", "600030": "大金融/券商",
    "600900": "高股息/电力", "601899": "资源/矿业", "600150": "中特估/船舶"
}

def format_ticker(code):
    if code.startswith('6'): return f"{code}.SS"
    if code.startswith('0') or code.startswith('3'): return f"{code}.SZ"
    if code.startswith('688'): return f"{code}.SS"
    if code.startswith('8') or code.startswith('4'): return f"{code}.BJ"
    return code

CORE_TICKERS = [format_ticker(t) for t in CORE_TICKERS_RAW]

# ==========================================
# 2. 深度净化工具
# ==========================================
def safe_val(v, is_num=True):
    try:
        if v is None: return 0.0 if is_num else ""
        if hasattr(v, 'iloc'): v = v.iloc[-1]
        val = float(v)
        return val if math.isfinite(val) else 0.0
    except:
        return 0.0 if is_num else str(v)

# ==========================================
# 3. 核心算法逻辑 (适配 A股 波动率)
# ==========================================
def calculate_v1000_nexus_ashare(df, benchmark_df):
    try:
        if len(df) < 60: return None
        # 确保数据为 Series
        close = df['Close'].squeeze()
        high = df['High'].squeeze()
        low = df['Low'].squeeze()
        curr_price = float(close.iloc[-1])
        
        # 1. 相对强度 (RS) - 对标沪深300
        bench_aligned = benchmark_df.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        # A股 RS 创新高通常是主升浪标志
        rs_nh_20 = bool(rs_line.iloc[-1] >= rs_line.tail(25).max())
        
        # 2. 紧致度 (A股版 VCP)
        # A股由于10%限制，收盘价标准差/均值更平滑
        tightness = float((close.tail(12).std() / close.tail(12).mean()) * 100)
        
        # 3. RS 评分 (滚动表现)
        def get_perf(days):
            if len(close) < days: return 0.0
            return (curr_price - close.iloc[-days]) / close.iloc[-days]
        
        # 加权：近1个月(21天)权重最高，反映 A股 短线爆发力
        rs_score = float(get_perf(21)*4 + get_perf(63)*2 + get_perf(126))

        signals, base_res = [], 0
        # 信号 A：奇点突破 (RS 新高 + 价格横盘)
        if rs_nh_20 and tightness < 2.5: 
            signals.append("👁️奇点"); base_res += 4
        # 信号 B：高位平台 (接近半年高点)
        h120 = float(high.tail(120).max())
        if curr_price >= h120 * 0.92: 
            signals.append("🚀高位"); base_res += 2
        # 信号 C：多头排列
        ma20 = close.rolling(20).mean().iloc[-1]
        if curr_price > ma20:
            base_res += 1
        
        # A股平均振幅 (ADR)
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr
        }
    except Exception as e:
        # print(f"计算报错: {e}")
        return None

# ==========================================
# 4. 主指挥引擎
# ==========================================
def run_v1000_ashare():
    start_time = time.time()
    print("🚀 V1000 [A股枢纽版] 启动分析...")

    try:
        # 下载数据 (沪深300作为基准)
        data = yf.download(CORE_TICKERS, period="1y", group_by='ticker', threads=True, progress=False)
        bench_df = yf.download("000300.SS", period="1y", progress=False)['Close'].squeeze()
        # 大盘情绪：沪深300当日涨跌
        mkt_change = ((bench_df.iloc[-1] - bench_df.iloc[-2]) / bench_df.iloc[-2]) * 100
    except Exception as e:
        print(f"❌ 数据下载失败: {e}"); return

    candidates = []
    sector_cluster = {}
    
    for t_full in CORE_TICKERS:
        try:
            t_raw = t_full.split('.')[0]
            df_t = data[t_full].dropna()
            if df_t.empty or len(df_t) < 40: continue
            
            res = calculate_v1000_nexus_ashare(df_t, bench_df)
            if res:
                res["Ticker"] = t_raw
                res["Sector"] = SECTOR_MAP.get(t_raw, "其他板块")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 当前市场无枢纽信号"); return

    # 排序：得分越高越靠前
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(15)
    
    final_list = []
    for _, row in sorted_df.iterrows():
        rating = "💎SSS 领涨" if row['Base_Res'] >= 5 else ("🔥强势" if row['RS_Score'] > 0.2 else "✅观察")
        sig_str = " + ".join(row['Signals']) if row['Signals'] else "📈 稳健趋势"
        cluster = f"{sector_cluster.get(row['Sector'], 1)}家异动"
        
        final_list.append([
            str(row['Ticker']),
            rating,
            sig_str,
            cluster,
            f"{round(row['ADR'], 2)}%", # A股看重弹性(ADR)
            round(float(row['Price']), 2),
            f"{round(float(row['Tightness']), 2)}%",
            round(float(row['RS_Score']), 2),
            str(row['Sector']),
            "A-Share"
        ])

    # 构造北京时间
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%m-%d %H:%M')
    mkt_status = "多头" if mkt_change > 0 else "空头"
    
    header = [
        ["🏰 V1000 A股枢纽控制台", "更新:", bj_now, "大盘:", f"{round(mkt_change,2)}%", mkt_status, "", "", "", ""],
        ["代码", "综合评级", "核心信号", "板块共振", "波动率AD", "现价", "收盘紧致度", "RS强度", "所属行业", "市场"]
    ]
    
    matrix = header + final_list
    
    try:
        # 清洗并发送
        clean_matrix = json.loads(json.dumps(matrix, default=lambda x: safe_val(x, is_num=False)))
        resp = requests.post(WEBAPP_URL, json=clean_matrix, timeout=15)
        print(f"🎉 A股数据同步成功！耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v1000_ashare()
