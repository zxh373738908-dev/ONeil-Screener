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
        # 统一使用 V39 标签
        return doc.worksheet("A-Share V39-Titan")
    except:
        return doc.add_worksheet(title="A-Share V39-Titan", rows=1000, cols=20)

# ==========================================
# 🧠 2. V39.1 核心算法 (保持泰坦引擎)
# ==========================================
def analyze_v39_titan(df, rs_val, mkt_cap, m_regime):
    try:
        sub_df = df.tail(200).copy()
        c = sub_df['Close'].values; h = sub_df['High'].values; l = sub_df['Low'].values
        v = sub_df['Volume'].values; o = sub_df['Open'].values
        price = c[-1]
        
        # --- A. VCP 3.0 ---
        v_std_3 = np.std((h[-3:] - l[-3:]) / l[-3:] * 100)
        v_std_10 = np.std((h[-10:] - l[-10:]) / l[-10:] * 100)
        is_coiling = v_std_3 < v_std_10 * 0.75
        
        # --- B. 黎明枢轴 ---
        pivot_p = np.max(h[-10:-1])
        is_break_pivot = price > pivot_p
        
        # --- C. 机构吸筹质量 ---
        entity_ratio = abs(c - o) / (h - l + 0.001)
        niv_short = (np.sign(c[-10:] - o[-10:]) * v[-10:] * entity_ratio[-10:]).sum()
        is_heavy_accum = niv_short > 0
        
        # --- D. 均线与空间 ---
        ma50 = sub_df['Close'].rolling(50).mean().iloc[-1]
        ma200 = sub_df['Close'].rolling(200).mean().iloc[-1]
        
        v_hist, bins = np.histogram(c[-120:], bins=50, weights=v[-120:])
        curr_idx = np.searchsorted(bins, price * 1.01)
        overhead = v_hist[curr_idx:]
        target_p = bins[curr_idx + np.argmax(overhead)] if len(overhead) > 0 else price * 1.12
        rr_ratio = (target_p - price) / (price - (price * 0.95)) # 简化的 R/R

        # 战法判定
        tag = "🛡️哨兵观察"
        if is_coiling and is_break_pivot and is_heavy_accum:
            tag = "🌅黎明起爆(枢轴穿透)"
        elif mkt_cap > 800e8 and niv_short > 0 and price > ma50:
            tag = "🛡️泰坦基石(机构锁仓)"
        elif price > ma50 and is_break_pivot and rs_val > 1.2:
            tag = "🌪️主升加速"

        # 评分
        score = (rs_val * 35) + (20 if is_heavy_accum else 0) + (20 if is_coiling else 0)
        if m_regime == "DOWN": score *= 0.8
        if price < ma200: score -= 20

        return tag, round(score, 1), round(pivot_p, 2), round(target_p, 2), round(rr_ratio, 2)
    except:
        return "ERR", 0, 0, 0, 0

# ==========================================
# 🚀 3. 主扫描流程 (修正写入逻辑)
# ==========================================
def run_v39_sentinel():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V39.1 Titan-Sentinel 启动...")

    # 1. 大盘探测
    try:
        idx = yf.download("000300.SS", period="100d", progress=False)
        idx_c = idx['Close'].iloc[:, 0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close']
        m_regime = "UP" if idx_c.iloc[-1] > idx_c.rolling(50).mean().iloc[-1] else "DOWN"
        print(f" -> 🚦 当前大盘环境: {m_regime}")
    except: return print("❌ 数据环境故障")

    # 2. TV 池子
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}],
               "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3]} for d in raw_data])
    except: return print("❌ TV 接口异常")

    # 3. 全息扫描
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    all_hits = []
    chunk_size = 60
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 分析进度: {i+1} ~ {min(i+chunk_size, len(tickers))}...")
        data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                df_h = data[t].dropna()
                if len(df_h) < 150: continue
                p = df_h['Close'].iloc[-1]
                rs_raw = (p / df_h['Close'].iloc[-120]) / (idx_c.iloc[-1] / idx_c.iloc[-120])
                
                c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                tag, score, pivot, target, rr = analyze_v39_titan(df_h, rs_raw, row_info['mkt'], m_regime)
                
                # 记录所有数据，不在此处 continue，方便后续提取哨兵名单
                all_hits.append({
                    "Ticker": c_code, "Name": row_info['name'], "泰坦分": score, "战术勋章": tag, 
                    "黎明枢轴": pivot, "盈亏比": rr, "目标价": target,
                    "行业": row_info['industry'], "RS强度": round(rs_raw, 2), "Price": round(float(p), 2)
                })
            except: continue

    # 4. 筛选与兜底逻辑
    sh = init_sheet(); sh.clear()
    
    # 尝试按原标准筛选
    threshold = 50 if m_regime == "DOWN" else 40
    final_list = [h for h in all_hits if h['泰坦分'] >= threshold]

    if not final_list:
        print("⚠️ 原标准无匹配，切换至【哨兵模式】：提取最强 RS 幸存者...")
        # 按照 RS 强度强制取前 30 名
        final_list = sorted(all_hits, key=lambda x: x['RS强度'], reverse=True)[:30]
        for item in final_list: 
            item['战术勋章'] = "🛡️逆势哨兵(火种)"
            item['泰坦分'] = "观察中"
        diag_msg = f"🚦 大盘{m_regime}中，严控风险。当前为【哨兵观察名单】。"
    else:
        diag_msg = f"✅ 发现 {len(final_list)} 个符合泰坦标准的优质目标。"

    # 5. 排序与写入
    res_df = pd.DataFrame(final_list)
    cols = ["Ticker", "Name", "泰坦分", "战术勋章", "黎明枢轴", "盈亏比", "目标价", "行业", "RS强度", "Price"]
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    
    # 更新诊断栏和时间戳
    sh.update_acell("L1", f"V39.1 状态: {diag_msg}")
    sh.update_acell("L2", f"Last Update (BJ): {now_str}")
    
    print(f"🎉 V39.1 任务完成！已强行同步 {len(res_df)} 条战报。")

if __name__ == "__main__":
    run_v39_sentinel()
