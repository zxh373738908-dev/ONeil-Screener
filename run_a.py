import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
import yfinance as yf

# 基础干扰屏蔽
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 配置中心
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try:
        return doc.worksheet("A-Share V39-Titan")
    except:
        return doc.add_worksheet(title="A-Share V39-Titan", rows=1000, cols=20)

# ==========================================
# 🧠 2. V39.0 “泰坦”统领演算算法
# ==========================================
def analyze_v39_titan(df, rs_val, mkt_cap, m_regime):
    try:
        sub_df = df.tail(200).copy()
        c = sub_df['Close'].values; h = sub_df['High'].values; l = sub_df['Low'].values
        v = sub_df['Volume'].values; o = sub_df['Open'].values
        price = c[-1]
        
        # --- A. VCP 3.0 阶梯紧致度 ---
        # 测量近 3 天对比近 10 天的波动率是否在“加速萎缩”
        v_std_3 = np.std((h[-3:] - l[-3:]) / l[-3:] * 100)
        v_std_10 = np.std((h[-10:] - l[-10:]) / l[-10:] * 100)
        is_coiling = v_std_3 < v_std_10 * 0.7 # 弹簧加速压紧
        
        # --- B. 黎明枢轴 (Dawn Pivot) ---
        # 寻找最近 10 个交易日的最高阻力位
        pivot_p = np.max(h[-10:-1])
        is_break_pivot = price > pivot_p
        
        # --- C. 机构吸筹质量 (NIV - Net Institutional Value) ---
        # 价格变动方向 * 成交量，但只算实体的“有效量”
        entity_ratio = abs(c - o) / (h - l + 0.001)
        niv = (np.sign(c - o) * v * entity_ratio).sum() # 过去 200 天净流入质量
        niv_short = (np.sign(c[-10:] - o[-10:]) * v[-10:] * entity_ratio[-10:]).sum()
        is_heavy_accumulation = niv_short > 0
        
        # --- D. 均线与阶位 ---
        ma50 = sub_df['Close'].rolling(50).mean().iloc[-1]
        ma200 = sub_df['Close'].rolling(200).mean().iloc[-1]
        dist_ma50 = (price / ma50 - 1) * 100
        
        # --- E. 止损与空间 ---
        tr = pd.concat([sub_df['High']-sub_df['Low'], abs(sub_df['High']-sub_df['Close'].shift())], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        stop_p = price - (atr * 1.5)
        
        v_hist, bins = np.histogram(c[-120:], bins=50, weights=v[-120:])
        curr_idx = np.searchsorted(bins, price * 1.01)
        overhead_bins = v_hist[curr_idx:]
        target_p = bins[curr_idx + np.argmax(overhead_bins)] if len(overhead_bins) > 0 else price * 1.15
        rr_ratio = (target_p - price) / (price - stop_p) if (price - stop_p) > 0 else 0

        # ==========================================
        # ⚔️ 战法分队：泰坦黎明核心勋章
        # ==========================================
        tag = "观察"
        # 1. 🌅 黎明起爆 (地量+紧致+过枢轴)
        if is_coiling and is_break_pivot and is_heavy_accumulation:
            tag = "🌅黎明起爆(枢轴穿透)"
        # 2. 🛡️ 泰坦基石 (大盘白马 + 机构吸筹质量优)
        elif mkt_cap > 1000e8 and niv_short > 0 and abs(dist_ma50) < 5:
            tag = "🛡️泰坦基石(机构锁仓)"
        # 3. 🌪️ 动能加速
        elif price > ma50 and is_break_pivot and rs_val > 1.2:
            tag = "🌪️主升加速"

        # --- F. 综合评分 (根据大盘环境动态调整) ---
        score = (rs_val * 30) + (20 if is_heavy_accumulation else 0) + (min(rr_ratio, 5) * 15) + (25 if is_coiling else 0)
        
        # 大盘环境惩罚机制
        if m_regime == "DOWN":
            if tag == "观察": score *= 0.5
            else: score *= 0.8 # 环境不好，分数普遍打折
        
        if price < ma200: score -= 20 # 处在 200 日线下方是硬伤

        return tag, round(score, 1), round(pivot_p, 2), round(target_p, 2), round(stop_p, 2), round(rr_ratio, 2)
    except:
        return "ERR", 0, 0, 0, 0, 0

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v39_titan():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V39.0 Titan-Dawn 启动 (大盘环境自适应)...")

    # 1. 探测大盘环境指数 (Regime Filter)
    try:
        idx = yf.download("000300.SS", period="100d", progress=False)
        idx_c = idx['Close'].iloc[:, 0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close']
        ma50_idx = idx_c.rolling(50).mean().iloc[-1]
        m_regime = "UP" if idx_c.iloc[-1] > ma50_idx else "DOWN"
        print(f" -> 🚦 当前大盘环境: {m_regime} (300指数 vs MA50)")
    except: return print("❌ 指数下载失败")

    # 2. TV 云端名册
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "volume", "close", "industry"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}], # 提高至 80 亿市值
        "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][5]} for d in raw_data])
    except: return print("❌ 接口故障")

    # 3. 扫描演算
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    final_list = []
    chunk_size = 60
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 泰坦全息分析: {i+1} ~ {min(i+chunk_size, len(tickers))}...")
        data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                df_h = data[t].dropna()
                if len(df_h) < 150: continue
                p = df_h['Close'].iloc[-1]
                rs_raw = (p / df_h['Close'].iloc[-120]) / (idx_c.iloc[-1] / idx_c.iloc[-120])
                
                c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                tag, score, pivot, target, stop, rr = analyze_v39_titan(df_h, rs_raw, row_info['mkt'], m_regime)
                
                # 只有及格的火种才允许上榜
                threshold = 50 if m_regime == "DOWN" else 40
                if score < threshold: continue

                final_list.append({
                    "Ticker": c_code, "Name": row_info['name'], "泰坦分": score, "战术勋章": tag, 
                    "黎明枢轴(过此买)": pivot, "盈亏比": rr, "目标价": target, "止损价": stop,
                    "市值(亿)": round(row_info['mkt']/1e8, 2), "行业": row_info['industry'], "RS强度": round(rs_raw, 2)
                })
            except: continue

    if not final_list: return
    res_df = pd.DataFrame(final_list)

    # 4. 板块协同度排名
    res_df['行业RS'] = res_df.groupby('行业')['RS强度'].transform('mean')
    res_df['战术勋章'] = res_df.apply(lambda x: f"🔥主线 | {x['战术勋章']}" if x['行业RS'] > res_df['行业RS'].quantile(0.8) else x['战术勋章'], axis=1)

    # 5. 写入与美化
    res_df = res_df.sort_values(by="泰坦分", ascending=False).head(60)
    sh = init_sheet(); sh.clear()
    cols = ["Ticker", "Name", "泰坦分", "战术勋章", "黎明枢轴(过此买)", "盈亏比", "目标价", "止损价", "市值(亿)", "行业", "RS强度"]
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V39.0 Titan Dawn | Market Regime: {m_regime} | {now_str}")

    print(f"✅ V39.0 泰坦任务完成！指挥部请查看黎明枢轴点。")

if __name__ == "__main__":
    run_v39_titan()
