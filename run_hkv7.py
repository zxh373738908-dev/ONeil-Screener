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
# 1. 配置中心 (港股适配版)
# ==========================================
# Google Apps Script 接收地址 (保持不变)
WEBAPP_URL = "您的_WEBAPP_URL"

# 港股核心领袖池 (0700腾讯, 3690美团, 9988阿里, 1211比亚迪, 1810小米等)
CORE_TICKERS_HK = [
    "0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK", 
    "9888.HK", "1024.HK", "9618.HK", "0941.HK", "2318.HK",
    "0388.HK", "0005.HK", "2015.HK", "2269.HK", "1177.HK", 
    "2331.HK", "2020.HK", "9999.HK", "6618.HK", "9626.HK"
]

SECTOR_MAP_HK = {
    "0700.HK": "互联网/社交", "3690.HK": "生活服务", "9988.HK": "电商/云",
    "1211.HK": "新能源车", "1810.HK": "消费电子", "1024.HK": "短视频",
    "9618.HK": "电商/物流", "0941.HK": "电信/红利", "2318.HK": "金融/保险",
    "0388.HK": "金融/交易所", "0005.HK": "金融/银行", "2015.HK": "新能源车",
    "2020.HK": "体育用品", "2331.HK": "体育用品", "9999.HK": "游戏/网易"
}

# ==========================================
# 2. 深度净化工具
# ==========================================
def safe_val(v, is_num=True):
    try:
        if v is None: return 0.0 if is_num else ""
        if hasattr(v, 'iloc'): v = v.iloc[-1] # 取最新值
        if isinstance(v, (np.floating, np.integer, float, int)):
            return float(v) if math.isfinite(v) else 0.0
        return str(v)
    except:
        return 0.0 if is_num else str(v)

# ==========================================
# 3. 核心算法 (针对港股波动优化)
# ==========================================
def calculate_hk_nexus(df, bench_df):
    try:
        if len(df) < 100: return None
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = float(close.iloc[-1])
        
        # 1. 相对强度 (对比恒生指数 ^HSI 或 恒生科技 ^HSTECH)
        # 腾讯这类股，RS线必须强于大盘
        bench_aligned = bench_df.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        # RS线创20日新高：代表大盘跌它横盘，或大盘平它领涨
        rs_nh_20 = bool(rs_line.iloc[-1] >= rs_line.tail(20).max()) if not rs_line.empty else False
        
        # 2. VCP 紧致度 (收缩判定)
        # 港股震荡大，紧致度是过滤垃圾波动、寻找大户洗盘的关键
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # 3. RS 评分 (IBD风格)
        def get_perf(d): 
            if len(close) < d: return 0.0
            return float((curr_price - close.iloc[-d]) / close.iloc[-d])
        rs_score = float(get_perf(60)*2 + get_ret(120)*1.5 + get_ret(250)) # 港股更看重中短期

        signals, base_res = [], 0
        # 奇点信号：RS新高且股价极度紧致 (Minervini VCP 逻辑)
        if rs_nh_20 and tightness < 1.5: 
            signals.append("👁️奇點觉醒")
            base_res += 4
        # 突破信号：股价在近半年高点附近
        if curr_price >= float(high.tail(120).max()) * 0.97: 
            signals.append("🚀巔峰突破")
            base_res += 2
        
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr, "RS_NH": rs_nh_20
        }
    except:
        return None

# ==========================================
# 4. 主指挥引擎
# ==========================================
def run_hk_commander():
    start_time = time.time()
    print("🚀 V1000 [港股领袖版] 启动...")

    try:
        # 下载数据 (对比基准使用恒生指数)
        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', threads=True, progress=False)
        bench_df = yf.download("^HSI", period="2y", progress=False)['Close'].dropna()
        # 港股VIX替代品 (恒指波幅指数)
        vhsi_raw = yf.download("^VHSI", period="5d", progress=False)['Close']
        vhsi = float(vhsi_raw.iloc[-1]) if not vhsi_raw.empty else 25.0
    except Exception as e:
        print(f"❌ 数据下载失败: {e}"); return

    candidates = []
    sector_cluster = {}
    
    for t in CORE_TICKERS_HK:
        try:
            df_t = data[t].dropna()
            if df_t.empty or len(df_t) < 60: continue
            
            res = calculate_hk_nexus(df_t, bench_df)
            if res:
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP_HK.get(t, "其他板块")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 今日暂无符合逻辑的标的"); return

    # 排序：优选 RS 评分高且有奇点信号的
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(15)
    final_list = []
    
    for i, row in sorted_df.reset_index().iterrows():
        t_code = str(row['Ticker']).replace(".HK", "") # 去掉后缀美化显示
        # 评级逻辑
        rating = "💎SSS 统帅" if row['Base_Res'] >= 4 else ("🔥强势股" if row['RS_Score'] > 0 else "✅关注")
        sig_str = " + ".join(row['Signals']) if row['Signals'] else "📈 稳定走强"
        cluster = f"{sector_cluster.get(row['Sector'], 1)}只联动"
        
        final_list.append([
            t_code,
            rating,
            sig_str,
            cluster,
            "★" if row['RS_NH'] else "-", # RS新高标志
            round(float(row['Price']), 2),
            f"{round(float(row['Tightness']), 2)}%",
            round(float(row['RS_Score']), 2),
            f"{round(float(row['ADR']), 2)}%",
            str(row['Sector'])
        ])

    # 构造表头
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    header = [
        ["🏰 V1000 港股巅峰统帅", "Update:", bj_now, "VHSI:", round(vhsi, 2), "", "", "", "", ""],
        ["代码", "评级", "信号", "行业集群", "RS新高", "现价", "紧致度", "RS强度", "ADR", "行业"]
    ]
    
    matrix = header + final_list
    
    try:
        payload = json.loads(json.dumps(matrix, default=lambda x: str(x)))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 港股同步完成！捕捉标的: {len(final_list)} 只 | 耗时: {round(time.time() - start_time, 2)}s")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_hk_commander()
