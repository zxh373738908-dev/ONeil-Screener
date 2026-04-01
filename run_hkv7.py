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
# 1. 配置中心 (使用您的新 URL)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxpcWyAOD41J5NcmnAU76TzPm2xKTdUmLNmvpsltYHZLN3eb0HSERRvSmLNrtrevLid/exec"

# 港股核心领袖池 (增加了一些活跃票)
CORE_TICKERS_HK = [
    "0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK", 
    "9888.HK", "1024.HK", "9618.HK", "0941.HK", "2318.HK",
    "0388.HK", "0005.HK", "2015.HK", "2269.HK", "1177.HK", 
    "2331.HK", "2020.HK", "9999.HK", "6618.HK", "9626.HK",
    "0857.HK", "0883.HK", "1398.HK", "0939.HK", "1299.HK"
]

SECTOR_MAP_HK = {
    "0700.HK": "互联网/社交", "3690.HK": "生活服务", "9988.HK": "电商/云",
    "1211.HK": "新能源车", "1810.HK": "消费电子", "1024.HK": "短视频",
    "9618.HK": "电商/物流", "0941.HK": "电信/红利", "2318.HK": "金融/保险",
    "0388.HK": "金融/交易所", "0005.HK": "金融/银行", "2015.HK": "新能源车",
    "2020.HK": "体育用品", "2331.HK": "体育用品", "9999.HK": "游戏/网易",
    "0857.HK": "能源/石油", "0883.HK": "能源/石油", "1299.HK": "保险/友邦"
}

# ==========================================
# 2. 深度净化工具 (防止 JSON 报错)
# ==========================================
def clean_for_json(val):
    """确保所有数据都是 Google Sheets 兼容的基础类型"""
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return ""
    if isinstance(val, (np.integer, np.floating)):
        return float(val)
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.strftime('%Y-%m-%d')
    return val

# ==========================================
# 3. 核心演算逻辑
# ==========================================
def calculate_hk_nexus(df, bench_df):
    try:
        if len(df) < 120: return None
        
        close = df['Close']
        high = df['High']
        low = df['Low']
        curr_price = float(close.iloc[-1])
        
        # 1. 相对强度 RS (对比恒指)
        bench_aligned = bench_df.reindex(close.index).ffill()
        rs_line = (close / bench_aligned).dropna()
        rs_nh_20 = bool(rs_line.iloc[-1] >= rs_line.tail(20).max()) if not rs_line.empty else False
        
        # 2. 紧致度 VCP
        tightness = float((close.tail(10).std() / close.tail(10).mean()) * 100)
        
        # 3. RS 评分
        def get_perf(d): 
            if len(close) < d: return 0.0
            prev = close.iloc[-d]
            return float((curr_price - prev) / prev) if prev != 0 else 0.0
        rs_score = (get_perf(63) * 2) + get_perf(126) + get_perf(252)

        # 4. 战法判定
        signals, base_res = [], 0
        if rs_nh_20 and tightness < 2.0: 
            signals.append("👁️奇點觉醒")
            base_res += 4
        if curr_price >= float(high.tail(126).max()) * 0.97: 
            signals.append("🚀巔峰突破")
            base_res += 2
        
        adr = float(((high - low) / low).tail(20).mean() * 100)
        ma50 = close.rolling(50).mean().iloc[-1]
        is_bull = curr_price > ma50

        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr, 
            "RS_NH": rs_nh_20, "is_bull": is_bull
        }
    except: return None

# ==========================================
# 4. 主程序
# ==========================================
def run_hk_commander():
    start_time = time.time()
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 🚀 V1000 港股领袖版启动...")

    try:
        data = yf.download(CORE_TICKERS_HK, period="2y", group_by='ticker', threads=True, progress=False)
        bench_df = yf.download("^HSI", period="2y", progress=False)['Close'].dropna()
        hsi_vol = bench_df.pct_change().tail(20).std() * math.sqrt(252) * 100
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    candidates = []
    sector_cluster = {}
    
    for t in CORE_TICKERS_HK:
        try:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if df_t.empty or len(df_t) < 60: continue
            
            res = calculate_hk_nexus(df_t, bench_df)
            # 只选多头排列且有动量的票
            if res and res["is_bull"] and (res["Base_Res"] > 0 or res["RS_Score"] > 0):
                res["Ticker"] = t
                res["Sector"] = SECTOR_MAP_HK.get(t, "其他")
                candidates.append(res)
                sector_cluster[res["Sector"]] = sector_cluster.get(res["Sector"], 0) + 1
        except: continue

    if not candidates:
        print("📭 今日暂无符合逻辑的标的 (建议观察大盘收盘)"); return

    # 排序
    sorted_df = pd.DataFrame(candidates).sort_values(by=["Base_Res", "RS_Score"], ascending=False).head(15)
    final_list = []
    
    for i, row in sorted_df.reset_index().iterrows():
        final_list.append([
            str(row['Ticker']).replace(".HK", ""),
            "💎SSS 统帅" if row['Base_Res'] >= 4 else "🔥强势股",
            " + ".join(row['Signals']) if row['Signals'] else "📈 稳定走强",
            f"{sector_cluster.get(row['Sector'], 1)}只联动",
            "★" if row['RS_NH'] else "-",
            round(float(row['Price']), 2),
            f"{round(float(row['Tightness']), 2)}%",
            round(float(row['RS_Score']), 2),
            f"{round(float(row['ADR']), 2)}%",
            str(row['Sector'])
        ])

    # 构造矩阵
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    header = [
        ["🏰 V1000 港股巅峰统帅", "Update:", bj_now, "HSI_Vol:", f"{round(hsi_vol, 2)}%", "", "", "", "", ""],
        ["代码", "评级", "信号", "行业集群", "RS新高", "现价", "紧致度", "RS强度", "ADR", "行业"]
    ]
    
    matrix = header + final_list
    
    # 最终净化并发送
    try:
        clean_matrix = [[clean_for_json(cell) for cell in row] for row in matrix]
        resp = requests.post(WEBAPP_URL, json=clean_matrix, timeout=15)
        
        if resp.status_code == 200 and "Success" in resp.text:
            print(f"🎉 港股同步成功！捕捉: {len(final_list)} 只 | 耗时: {round(time.time() - start_time, 2)}s")
        else:
            print(f"⚠️ 脚本已发送但服务器返回: {resp.text}")
    except Exception as e:
        print(f"❌ 同步请求失败: {e}")

if __name__ == "__main__":
    run_hk_commander()
