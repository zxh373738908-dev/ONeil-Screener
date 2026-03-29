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
        return doc.worksheet("A-Share V26-Spark")
    except:
        return doc.add_worksheet(title="A-Share V26-Spark", rows=1000, cols=20)

# ==========================================
# 🧠 2. V26.0 火种决策引擎
# ==========================================
def analyze_v26_logic(df, rs_raw_val, sector_bonus, breadth_factor):
    try:
        c = df['Close'].values; h = df['High'].values; l = df['Low'].values; v = df['Volume'].values
        price = c[-1]
        ma20 = df['Close'].rolling(20).mean().iloc[-1]
        ma200 = df['Close'].rolling(200).mean().iloc[-1]
        h250 = np.max(h[-250:])
        
        dist_high = (price / h250 - 1) * 100
        ext_20 = (price / ma20 - 1) * 100
        
        # --- A. 信号定义 ---
        tag = "🔍 观察"
        # 1. 相对强度领先 (即便在跌，也比大盘硬)
        if rs_raw_val > 1.1: tag = "💎 相对强势"
        
        # 2. 超跌火种探测 (偏离均线太远且放量)
        avg_v50 = np.mean(v[-50:])
        if ext_20 < -15 and v[-1] > avg_v50 * 1.5:
            tag = "🌋 底部火种"
        
        # 3. 趋势幸存者
        if price > ma20 and price > ma200:
            tag = "🛡️ 趋势幸存"

        # --- B. 评分逻辑 (不设截断) ---
        score = (rs_raw_val * 60) + sector_bonus
        if "幸存" in tag: score += 20
        if "强势" in tag: score += 10
        
        final_score = score * (breadth_factor + 0.2) # 冰点期给予一定系数补偿
        
        return tag, round(final_score, 1), round(dist_high, 1), round(ext_20, 1)
    except:
        return "ERR", 0, 0, 0

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v26_spark():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V26.0 Spark 启动 (冰点强制选股模式)...")

    # 1. 广度探测
    try:
        idx = yf.download("000300.SS", period="350d", progress=False)
        idx_c = idx['Close'].iloc[:, 0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close']
    except: return print("❌ 无法获取大盘指数")

    # 2. 获取池子
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "volume", "close", "industry", "change"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 7e9}], # 降低市值门槛至70亿
        "range": [0, 600], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "industry": d['d'][5], "chg": d['d'][6]} for d in raw_data])
    except: return print("❌ 接口异常")

    # 3. 广度因子
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    breadth_check = yf.download(tickers[:100], period="250d", progress=False)['Close']
    uptrend_count = sum([1 for t in breadth_check.columns if breadth_check[t].iloc[-1] > breadth_check[t].rolling(200).mean().iloc[-1]])
    breadth_factor = max(0.3, min(1.0, (uptrend_count / 100) * 1.5))

    # 4. 演算
    final_list = []
    chunk_size = 60
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 扫描进度: {i+1} ~ {min(i+chunk_size, len(tickers))}...")
        data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                df_h = data[t].dropna()
                if len(df_h) < 200: continue
                p = df_h['Close'].iloc[-1]
                rs_raw = (p / df_h['Close'].iloc[-250]) / (idx_c.iloc[-1] / idx_c.iloc[-250])
                
                c_code = t.split('.')[0]
                row = df_pool[df_pool['code'] == c_code].iloc[0]
                
                tag, score, d_high, ext_20 = analyze_v26_logic(df_h, rs_raw, 0, breadth_factor)
                
                # 无差别入榜，后续统一排序
                final_list.append({
                    "Ticker": c_code, "Name": row['name'], "综合评分": score, "勋章": tag, 
                    "行业": row['industry'], "RS评级": rs_raw, "距高点%": d_high, "MA20乖离%": ext_20, "Price": round(float(p), 2)
                })
            except: continue

    # 5. 写入 Google Sheets
    sh = init_sheet(); sh.clear()
    
    if not final_list:
        sh.update_acell("A1", "全场无数据，请检查网络。")
        return

    res_df = pd.DataFrame(final_list)
    # 计算 RS 百分比排名
    res_df['RS排名'] = res_df['RS评级'].rank(pct=True).apply(lambda x: int(x*99))
    # 强制排序：无论行情多烂，取综合评分前 60 名
    res_df = res_df.sort_values(by="综合评分", ascending=False).head(60)

    cols = ["Ticker", "Name", "综合评分", "勋章", "行业", "RS排名", "距高点%", "MA20乖离%", "Price"]
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    
    status = "❄️ 极寒" if uptrend_count < 40 else "🔥 活跃"
    sh.update_acell("J1", f"气象站 | 广度: {uptrend_count}% | 状态: {status} | 强制选股模式已开启")

    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            # 对 RS 排名 90 以上的加红
            rule_rs = ConditionalFormatRule(ranges=[GridRange.from_a1_range('F2:F60', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER_OR_EQUAL', ['90']),
                    format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1, 0, 0)))))
            fmt_rules.append(rule_rs)
            fmt_rules.save()
        except: pass
    
    print(f"✅ V26.0 Spark 任务完成！已强行从寒冬中提取 {len(res_df)} 个火种。")

if __name__ == "__main__":
    run_v26_spark()
