import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import math
import warnings

# 禁用警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (使用您的最新 URL)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbzTHj8yuS0VLfqPSjj8FzqjumjWOVRRignL0lP2gRfQMlt8k7szpS9sq3HtVaQ4HhXY/exec"

# 港股核心领袖池 (覆盖科技、能源、金融、消费)
CORE_TICKERS_HK = [
    "0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK", 
    "9888.HK", "1024.HK", "9618.HK", "0941.HK", "2318.HK",
    "0388.HK", "0005.HK", "2015.HK", "2269.HK", "1177.HK", 
    "2331.HK", "2020.HK", "9999.HK", "6618.HK", "9626.HK",
    "0857.HK", "0883.HK", "1398.HK", "0939.HK", "1299.HK",
    "2317.HK", "1880.HK", "0016.HK", "0669.HK", "1088.HK"
]

SECTOR_MAP_HK = {
    "0700.HK": "互联网/社交", "3690.HK": "生活服务", "9988.HK": "电商/云",
    "1211.HK": "新能源车", "1810.HK": "消费电子", "1024.HK": "短视频",
    "9618.HK": "电商/物流", "0941.HK": "电信/红利", "2318.HK": "金融/保险",
    "0388.HK": "金融/交易所", "0005.HK": "金融/银行", "2015.HK": "新能源车",
    "2020.HK": "体育用品", "2331.HK": "体育用品", "9999.HK": "游戏/网易",
    "0857.HK": "能源/石油", "0883.HK": "能源/石油", "1299.HK": "保险/友邦",
    "1088.HK": "能源/煤炭", "0669.HK": "创科实业"
}

# ==========================================
# 2. 数据净化工具 (防止 JSON 序列化失败)
# ==========================================
def clean_for_json(val):
    if val is None: return ""
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val): return ""
        return round(val, 2)
    if isinstance(val, (np.integer, np.floating)):
        val = float(val)
        return round(val, 2) if math.isfinite(val) else ""
    return str(val)

# ==========================================
# 3. V1000 核心演算逻辑
# ==========================================
def calculate_hk_nexus(df, bench_df):
    try:
        if len(df) < 100: return None
        
        close = df['Close']
        high = df['High']
        low = df['Low']
        curr_price = float(close.iloc[-1])
        
        # 1. 相对强度 RS (对比恒生指数)
        bench_aligned = bench_df.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        rs_nh_20 = bool(rs_line.iloc[-1] >= rs_line.tail(20).max()) if not rs_line.empty else False
        
        # 2. VCP 紧致度 (收缩判定)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # 3. RS 动量评分 (IBD 风格)
        def get_perf(d): 
            if len(close) < d: return 0.0
            prev = close.iloc[-d]
            return float((curr_price - prev) / prev) if prev != 0 else 0.0
        # 评分：近3个月表现最重要
        rs_score = (get_perf(63) * 2) + get_perf(126) + get_perf(252)

        # 4. 战法信号
        signals, base_res = [], 0
        # 信号：奇点觉醒 (RS走强 + 股价横盘收缩)
        if rs_nh_20 and tightness < 2.2: 
            signals.append("👁️奇點觉醒")
            base_res += 4
        # 信号：巅峰突破 (距离半年高点 3% 以内)
        half_year_max = float(high.tail(126).max())
        if curr_price >= half_year_max * 0.97: 
            signals.append("🚀巔峰突破")
            base_res += 2
        
        # 5. 趋势过滤 (必须在 50 日均线上方)
        ma50 = close.rolling(50).mean().iloc[-1]
        is_bull = curr_price > ma50
        
        # 平均波幅 ADR
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr, 
            "RS_NH": rs_nh_20, "is_bull": is_bull
        }
    except: return None

# ==========================================
# 4. 执行引擎
# ==========================================
def run_hk_commander():
    start_time = time.time()
    now_ts = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_ts}] 🛰️ V1000 港股统帅启动审计...")

    try:
        # 下载数据
        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', threads=True, progress=False)
        bench_df = yf.download("^HSI", period="2y", progress=False)['Close'].dropna()
        # 计算恒指 20 日波动率
        hsi_vol = bench_df.pct_change().tail(20).std() * math.sqrt(252) * 100
    except Exception as e:
        print(f"❌ 基础数据下载失败: {e}"); return

    candidates = []
    sector_cluster = {}
    
    for t in CORE_TICKERS_HK:
        try:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if df_t.empty or len(df_t) < 60: continue
            
            res = calculate_hk_nexus(df_t, bench_df)
            # 过滤条件：多头趋势 + 有信号或动量为正
            if res and res["is_bull"] and (res["Base_Res"] > 0 or res["RS_Score"] > 0):
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP_HK.get(t, "其他板块")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 今日暂无符合逻辑的标的 (建议观察大盘支撑位)"); return

    # 排序：战法优先，动量其次
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(15)
    
    # 格式化输出矩阵
    final_list = []
    for i, row in sorted_df.reset_index().iterrows():
        final_list.append([
            str(row['Ticker']).replace(".HK", ""),
            "💎SSS 统帅" if row['Base_Res'] >= 4 else "🔥强势股",
            " + ".join(row['Signals']) if row['Signals'] else "📈 趋势保持",
            f"{sector_cluster.get(row['Sector'], 1)}只联动",
            "★" if row['RS_NH'] else "-",
            row['Price'],
            f"{round(row['Tightness'], 2)}%",
            row['RS_Score'],
            f"{round(row['ADR'], 2)}%",
            row['Sector']
        ])

    # 构造完整矩阵
    header = [
        ["🏰 V1000 港股巅峰统帅", "更新时间:", now_ts, "恒指波幅:", f"{round(hsi_vol, 2)}%", "", "", "", "", ""],
        ["代码", "评级", "核心信号", "行业集群", "RS新高", "现价", "紧致度", "RS强度评分", "ADR波幅", "所属行业"]
    ]
    matrix = header + final_list
    
    # 数据清洗并发送
    try:
        clean_matrix = [[clean_for_json(cell) for cell in row] for row in matrix]
        resp = requests.post(WEBAPP_URL, json=clean_matrix, timeout=20)
        
        print(f"DEBUG: 服务器返回 -> {resp.text}")
        if resp.text == "Success":
            print(f"🎉 港股同步成功！捕捉: {len(final_list)} 只标的 | 耗时: {round(time.time() - start_time, 2)}s")
        else:
            print(f"⚠️ 同步失败，原因: {resp.text}")
    except Exception as e:
        print(f"❌ 同步请求异常: {e}")

if __name__ == "__main__":
    run_hk_commander()
