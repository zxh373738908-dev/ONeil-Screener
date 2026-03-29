import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
import yfinance as yf

# 尝试导入美化插件
try:
    from gspread_formatting import *
    HAS_FORMATTING = True
except ImportError:
    HAS_FORMATTING = False

# 基础屏蔽
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
        # 修改表名为 V25
        return doc.worksheet("A-Share V25-Watcher")
    except:
        return doc.add_worksheet(title="A-Share V25-Watcher", rows=1000, cols=20)

# ==========================================
# 🧠 2. V25.0 守望者演算引擎
# ==========================================
def analyze_v25_logic(df, rs_raw_val, sector_rank_bonus, breadth_factor):
    try:
        sub_df = df.tail(15)
        c = sub_df['Close'].values; h = sub_df['High'].values; l = sub_df['Low'].values
        v = sub_df['Volume'].values; o = sub_df['Open'].values
        
        price = c[-1]
        ma20 = df['Close'].rolling(20).mean().iloc[-1]
        h250 = np.max(df['High'].tail(250))
        dist_high = (price / h250 - 1) * 100
        
        # --- A. 信号探测 ---
        tag = "趋势守望"
        is_hard_signal = False
        
        # 1. 回溯 3 日的口袋买点 (Pocket Pivot)
        for i in range(-1, -4, -1):
            rets_i = np.diff(df['Close'].iloc[i-11:i+1].values) / df['Close'].iloc[i-12:i].values
            vols_i = df['Volume'].iloc[i-11:i].values
            down_vols = [vols_i[j] for j in range(10) if rets_i[j] < 0]
            max_down_v = max(down_vols) if down_vols else 999999999
            
            if c[i] > o[i] and v[i] > max_down_v:
                tag = "✨起爆信号" if i == -1 else f"⚡{abs(i)-1}日前起爆"
                is_hard_signal = True
                break
        
        # 2. 韧性检测 (如果没信号但极度抗跌)
        if not is_hard_signal:
            if price > ma20 and dist_high > -5:
                tag = "🛡️逆势韧性"
            else:
                tag = "🔎潜力观察"

        # --- B. 评分逻辑 ---
        score = (rs_raw_val * 50) + sector_rank_bonus
        if is_hard_signal: score += 20
        if tag == "🛡️逆势韧性": score += 10
        
        final_score = score * breadth_factor
        
        return tag, round(final_score, 1), dist_high
    except:
        return "ERR", 0, 0

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v25_watcher():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V25.0 Watcher 启动...")

    # 1. 广度探测
    try:
        idx = yf.download("000300.SS", period="350d", progress=False)
        idx_c = idx['Close'].iloc[:, 0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close']
    except: return print("❌ 数据获取失败")

    # 2. 获取 TV 池子 (适当扩大范围至 800 只)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "volume", "close", "industry", "change"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 8e9}],
        "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "industry": d['d'][5], "chg": d['d'][6], "mkt": d['d'][2]} for d in raw_data])

    # 3. 广度与行业
    sector_perf = df_pool.groupby('industry')['chg'].mean().sort_values(ascending=False)
    top_sectors = sector_perf.head(max(1, len(sector_perf)//5)).index.tolist()
    
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    breadth_check = yf.download(tickers[:100], period="250d", progress=False)['Close']
    uptrend_count = sum([1 for t in breadth_check.columns if breadth_check[t].iloc[-1] > breadth_check[t].rolling(200).mean().iloc[-1]])
    breadth_factor = max(0.4, min(1.0, (uptrend_count / 100) * 1.5))

    # 4. 演算
    final_list = []
    chunk_size = 60
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 扫描进度: {i+1} ~ {min(i+chunk_size, len(tickers))}...")
        data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                df_h = data[t].dropna()
                if len(df_h) < 200: continue
                p = df_h['Close'].iloc[-1]
                rs_raw = (p / df_h['Close'].iloc[-250]) / (idx_c.iloc[-1] / idx_c.iloc[-250])
                
                c_code = t.split('.')[0]
                row = df_pool[df_pool['code'] == c_code].iloc[0]
                bonus = 15 if row['industry'] in top_sectors else 0
                
                tag, score, d_high = analyze_v25_logic(df_h, rs_raw, bonus, breadth_factor)
                
                # V25 降级逻辑：如果在冰点期，且没有爆款信号，只要 score > 30 且抗跌就入榜观察
                if score > 35 or (uptrend_count < 50 and score > 28 and d_high > -8):
                    final_list.append({
                        "Ticker": c_code, "Name": row['name'], "综合评分": score, "勋章": tag, 
                        "行业": row['industry'], "RS评级": rs_raw, "距高点%": round(d_high, 2), "Price": round(float(p), 2)
                    })
            except: continue

    # 5. 写入
    sh = init_sheet(); sh.clear()
    
    if not final_list:
        sh.update_acell("A1", f"⚠️ 极端风险：广度仅 {uptrend_count}%，全场无任何抗跌标的。")
        return

    res_df = pd.DataFrame(final_list)
    res_df['RS评级'] = res_df['RS评级'].rank(pct=True).apply(lambda x: int(x*99))
    # 排序：评分优先，距高点近优先
    res_df = res_df.sort_values(by=["综合评分", "距高点%"], ascending=[False, False]).head(60)

    cols = ["Ticker", "Name", "综合评分", "勋章", "行业", "RS评级", "距高点%", "Price"]
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    
    # 表头预警
    risk_level = "🔴 极高" if uptrend_count < 40 else ("🟡 中等" if uptrend_count < 60 else "🟢 较低")
    header_msg = f"气象站 | 广度: {uptrend_count}% | 风险等级: {risk_level} | Updated: {now_str}"
    sh.update_acell("I1", header_msg)

    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            # 给抗跌韧性的打上蓝色背景
            rule_tough = ConditionalFormatRule(ranges=[GridRange.from_a1_range('D2:D60', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['韧性']),
                    format=cellFormat(backgroundColor=color(0.9, 0.95, 1))))
            fmt_rules.append(rule_tough)
            fmt_rules.save()
        except: pass
    
    print(f"✅ V25.0 Watcher 任务完成！当前行情风险等级: {risk_level}")

if __name__ == "__main__":
    run_v25_watcher()
