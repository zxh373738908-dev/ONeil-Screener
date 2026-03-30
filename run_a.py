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
        return doc.worksheet("A-Share V43-Celestial")
    except:
        return doc.add_worksheet(title="A-Share V43-Celestial", rows=1000, cols=20)

# ==========================================
# 🧠 2. V43.0 多维导航决策引擎
# ==========================================
def analyze_v43_celestial(df, mkt_cap, s_alpha):
    try:
        # 取核心数据
        sub_df = df.tail(250).copy()
        c = sub_df['Close'].values; h = sub_df['High'].values; l = sub_df['Low'].values
        v = sub_df['Volume'].values; o = sub_df['Open'].values
        price = c[-1]
        
        # --- A. 均线与位置 ---
        ma50 = sub_df['Close'].rolling(50).mean().iloc[-1]
        ma200 = sub_df['Close'].rolling(200).mean().iloc[-1]
        h52 = np.max(h); l52 = np.min(l)
        range_pos = (price - l52) / (h52 - l52) * 100
        
        # --- B. 信号回溯探测 (检测近3日) ---
        has_pocket = False
        for i in range(-1, -4, -1):
            # 口袋买点逻辑：上涨且量 > 过去10天最大阴线量
            rets_i = np.diff(sub_df['Close'].iloc[i-11:i+1].values) / sub_df['Close'].iloc[i-12:i].values
            vols_i = sub_df['Volume'].iloc[i-11:i].values
            down_vols = [vols_i[j] for j in range(10) if rets_i[j] < 0]
            max_down_v = max(down_vols) if down_vols else 999999999
            if c[i] > o[i] and v[i] > max_down_v:
                has_pocket = True; break
        
        # --- C. OBV 潜伏探测 (针对 600519) ---
        obv = (np.sign(np.diff(c)) * v[1:]).cumsum()
        is_obv_accel = obv[-1] > obv[-5:].mean() > obv[-20:].mean()
        
        # --- D. 紧致度 ---
        vcp_idx = np.std((h[-10:] - l[-10:]) / l[-10:] * 100) / np.std((h[-50:] - l[-50:]) / l[-50:] * 100)

        # ==========================================
        # ⚔️ 战法识别体系
        # ==========================================
        tag = "持有/观察"
        # 1. 🛡️ 白马觉醒 (提早锁定 600519)
        if mkt_cap > 1000e8 and is_obv_accel and (price > ma50 or abs(price/ma50-1)<0.03):
            tag = "🛡️白马觉醒(潜伏)"
        # 2. ⚡ 信号回溯 (最近3天有过起爆)
        elif has_pocket and range_pos > 60:
            tag = "⚡近期曾起爆"
        # 3. ✨ 极致紧致
        elif vcp_idx < 0.5 and price > ma50:
            tag = "✨极致紧致(爆点)"

        # 评分
        score = (s_alpha * 20) + (30 if has_pocket else 0) + (25 if is_obv_accel else 0) + (max(0, (1-vcp_idx)*20))
        if tag == "🛡️白马觉醒(潜伏)": score += 20
        if price < ma200: score -= 15

        return tag, round(score, 1), round(vcp_idx, 2), range_pos
    except:
        return "ERR", 0, 0, 0

# ==========================================
# 🚀 3. 主扫描流程 (永不落空机制)
# ==========================================
def run_v43_celestial():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V43.0 天象导航启动 (多维回溯+蓝筹潜伏)...")

    cols = ["Ticker", "Name", "综合分", "RS评级", "战术勋章", "52周位置%", "VCP指数", "行业", "市值(亿)", "Price"]

    # 1. 获取 TV 名册
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "change"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 65e8}],
               "range": [0, 850], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    
    try:
        raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "chg": d['d'][4]} for d in raw_data])
        sector_alpha = df_pool.groupby('industry')['chg'].mean().to_dict()
    except: return print("❌ 接口异常")

    # 2. 基准
    idx = yf.download("000300.SS", period="300d", progress=False)
    idx_c = idx['Close'].iloc[:, 0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close']

    # 3. 扫描演算
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    all_hits = []
    chunk_size = 30
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 导航演算区块 {i//chunk_size + 1}...")
        try:
            data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True, timeout=10)
            for t in chunk:
                try:
                    if t not in data.columns.get_level_values(0): continue
                    df_h = data[t].dropna()
                    if len(df_h) < 150: continue
                    
                    p = df_h['Close'].iloc[-1]
                    rs_raw = (p / df_h['Close'].iloc[-120]) / (idx_c.iloc[-1] / idx_c.iloc[-120])
                    
                    c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                    s_alpha = sector_alpha.get(row_info['industry'], 0)
                    
                    tag, score, vcp, r_pos = analyze_v43_celestial(df_h, row_info['mkt'], s_alpha)
                    
                    all_hits.append({
                        "Ticker": c_code, "Name": row_info['name'], "综合分": score, "战术勋章": tag, 
                        "52周位置%": round(r_pos, 1), "VCP指数": vcp, "行业": row_info['industry'], 
                        "RS强度": rs_raw, "市值(亿)": round(row_info['mkt']/1e8, 2), "Price": round(float(p), 2)
                    })
                except: continue
        except: continue

    # 4. 写入与兜底排名 (永不空回逻辑)
    sh = init_sheet(); sh.clear()
    
    if not all_hits:
        sh.update_acell("A1", "🚨 数据源暂时不可达，请 10 分钟后重试。")
        return

    res_df = pd.DataFrame(all_hits)
    res_df['RS评级'] = res_df['RS强度'].rank(pct=True).apply(lambda x: int(x*99))
    
    # --- 核心：如果高标太少，自动执行“哨兵强制入榜” ---
    high_quality = res_df[res_df['综合分'] >= 50]
    if len(high_quality) < 15:
        print("⚠️ 优质信号较少，激活【哨兵侦查】模式...")
        final_df = res_df.sort_values(by=["RS强度", "综合分"], ascending=False).head(50)
        status_msg = f"❄️ 当前行情极寒。显示全市场最强 RS 的 50 名【逆境哨兵】。 | {now_str}"
    else:
        final_df = res_df.sort_values(by="综合分", ascending=False).head(60)
        status_msg = f"🔥 发现 {len(high_quality)} 个优质目标，已按综合战力排序。 | {now_str}"

    # 5. 安全写入
    sh.update(range_name="A1", values=[cols] + final_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", status_msg)
    
    print(f"✅ V43.0 导航任务大功告成！")

if __name__ == "__main__":
    run_v43_celestial()
