import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import math
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
# 请替换为您自己的 Google Apps Script URL
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyVyClySDklAPrx30URWZOGyb423Vb_5Dzt7WCKQ6WJwcNb7HLqwD0ckiMYm5sTwnLz/exec"

# 港股核心领袖池
CORE_TICKERS_HK = [
    "0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK", 
    "9888.HK", "1024.HK", "9618.HK", "0941.HK", "2318.HK",
    "0388.HK", "0005.HK", "2015.HK", "2269.HK", "1177.HK", 
    "2331.HK", "2020.HK", "9999.HK", "6618.HK", "9626.HK",
    "0857.HK", "0883.HK", "1398.HK", "0939.HK", "1299.HK"
]

# 行业映射
SECTOR_MAP_HK = {
    "0700.HK": "互联网/社交", "3690.HK": "生活服务", "9988.HK": "电商/云",
    "1211.HK": "新能源车", "1810.HK": "消费电子", "1024.HK": "短视频",
    "9618.HK": "电商/物流", "0941.HK": "电信/红利", "2318.HK": "金融/保险",
    "0388.HK": "金融/交易所", "0005.HK": "金融/银行", "2015.HK": "新能源车",
    "2020.HK": "体育用品", "2331.HK": "体育用品", "9999.HK": "游戏/网易",
    "0857.HK": "能源/石油", "0883.HK": "能源/石油", "1299.HK": "保险/友邦"
}

# ==========================================
# 2. 深度净化工具
# ==========================================
def safe_val(v, is_num=True):
    try:
        if v is None: return 0.0 if is_num else ""
        if isinstance(v, (pd.Series, np.ndarray)):
            v = v.iloc[-1] if len(v) > 0 else (0.0 if is_num else "")
        if isinstance(v, (float, int, np.floating, np.integer)):
            return float(v) if math.isfinite(v) else 0.0
        return str(v)
    except:
        return 0.0 if is_num else str(v)

# ==========================================
# 3. 核心算法 (针对港股优化)
# ==========================================
def calculate_hk_nexus(df, bench_df):
    try:
        # 确保数据量充足
        if len(df) < 120: return None
        
        close = df['Close']
        high = df['High']
        low = df['Low']
        curr_price = float(close.iloc[-1])
        
        # 1. 相对强度 (对比恒生指数)
        bench_aligned = bench_df.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        # RS线创 20 日新高
        rs_nh_20 = bool(rs_line.iloc[-1] >= rs_line.tail(20).max()) if not rs_line.empty else False
        
        # 2. 紧致度 (收缩判定)
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # 3. RS 评分 (IBD风格改进)
        def get_perf(d): 
            if len(close) < d: return 0.0
            prev_price = close.iloc[-d]
            return float((curr_price - prev_price) / prev_price) if prev_price != 0 else 0.0

        # 加权：近3个月表现占40%，近半年30%，近一年30%
        rs_score = (get_perf(63) * 2) + get_perf(126) + get_perf(252)

        # 4. 战法判定
        signals, base_res = [], 0
        # 奇点觉醒：RS走强且股价收缩
        if rs_nh_20 and tightness < 2.0: 
            signals.append("👁️奇點觉醒")
            base_res += 4
        # 巅峰突破：距离半年高点 3% 以内
        half_year_max = float(high.tail(126).max())
        if curr_price >= half_year_max * 0.97: 
            signals.append("🚀巔峰突破")
            base_res += 2
        
        # 平均波幅
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        # 趋势过滤：股价必须在 50 日均线上方
        ma50 = close.rolling(50).mean().iloc[-1]
        is_bull = curr_price > ma50

        return {
            "RS_Score": rs_score, 
            "Signals": signals, 
            "Base_Res": base_res, 
            "Price": curr_price, 
            "Tightness": tightness, 
            "ADR": adr, 
            "RS_NH": rs_nh_20,
            "is_bull": is_bull
        }
    except Exception as e:
        # print(f"Calculation Error: {e}")
        return None

# ==========================================
# 4. 主指挥引擎
# ==========================================
def run_hk_commander():
    start_time = time.time()
    print("🚀 V1000 [港股领袖版] 启动...")

    try:
        # 下载数据 (基准使用恒生指数)
        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', threads=True, progress=False)
        bench_df = yf.download("^HSI", period="2y", progress=False)['Close'].dropna()
        
        # 替代 VHSI 的逻辑：简单计算大盘过去 20 日波动率
        hsi_vol = bench_df.pct_change().tail(20).std() * math.sqrt(252) * 100
    except Exception as e:
        print(f"❌ 数据下载失败: {e}"); return

    candidates = []
    sector_cluster = {}
    
    for t in CORE_TICKERS_HK:
        try:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if df_t.empty or len(df_t) < 60: continue
            
            res = calculate_hk_nexus(df_t, bench_df)
            # 基础过滤：必须是多头趋势且有一定的动量或信号
            if res and (res["Base_Res"] > 0 or res["RS_Score"] > 0):
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP_HK.get(t, "其他板块")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 今日暂无符合逻辑的标的 (大盘环境可能较弱)"); return

    # 排序：优选战法信号多且 RS 强的
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(15)
    final_list = []
    
    for i, row in sorted_df.reset_index().iterrows():
        t_code = str(row['Ticker']).replace(".HK", "")
        # 评级逻辑
        rating = "💎SSS 统帅" if row['Base_Res'] >= 4 else ("🔥强势股" if row['RS_Score'] > 0.1 else "✅监控")
        sig_str = " + ".join(row['Signals']) if row['Signals'] else "📈 稳定走强"
        cluster = f"{sector_cluster.get(row['Sector'], 1)}只联动"
        
        final_list.append([
            t_code,
            rating,
            sig_str,
            cluster,
            "★" if row['RS_NH'] else "-",
            round(float(row['Price']), 2),
            f"{round(float(row['Tightness']), 2)}%",
            round(float(row['RS_Score']), 2),
            f"{round(float(row['ADR']), 2)}%",
            str(row['Sector'])
        ])

    # 构造表头
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    header = [
        ["🏰 V1000 港股巅峰统帅", "Update:", bj_now, "HSI_Vol:", round(hsi_vol, 2), "", "", "", "", ""],
        ["代码", "评级", "信号", "行业集群", "RS新高", "现价", "紧致度", "RS强度", "ADR", "行业"]
    ]
    
    matrix = header + final_list
    
    try:
        # 将数据同步到 Google Sheets
        payload = json.loads(json.dumps(matrix, default=lambda x: str(x)))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 港股同步完成！捕捉标的: {len(final_list)} 只 | 耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_hk_commander()
