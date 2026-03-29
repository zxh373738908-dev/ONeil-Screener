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
        return doc.worksheet("A-Share V24-Pathfinder")
    except:
        return doc.add_worksheet(title="A-Share V24-Pathfinder", rows=1000, cols=20)

# ==========================================
# 🧠 2. V24.0 探路者演算引擎 (增强回溯功能)
# ==========================================
def analyze_v24_logic(df, rs_raw_val, sector_rank_bonus, breadth_factor):
    """
    增加了 Lookback (回溯) 机制，支持扫描最近 3 天内的信号
    """
    try:
        # 提取最近 5 天数据用于分析
        sub_df = df.tail(15)
        c = sub_df['Close'].values; h = sub_df['High'].values; l = sub_df['Low'].values
        v = sub_df['Volume'].values; o = sub_df['Open'].values
        
        # 基础计算 (针对最后一天)
        price = c[-1]
        ma20 = df['Close'].rolling(20).mean().iloc[-1]
        ma50 = df['Close'].rolling(50).mean().iloc[-1]
        avg_v50 = np.mean(df['Volume'].tail(50))
        
        # --- A. 核心信号探测 (支持回溯 3 日) ---
        found_signal = False
        tag = "趋势跟踪"
        
        for i in range(-1, -4, -1):  # 检查最后 3 天
            # 1. 机构口袋买点 (Pocket Pivot)
            # 当前上涨且量 > 过去10天最大阴线量
            rets_i = np.diff(df['Close'].iloc[i-11:i+1].values) / df['Close'].iloc[i-12:i].values
            vols_i = df['Volume'].iloc[i-11:i].values
            down_vols = [vols_i[j] for j in range(10) if rets_i[j] < 0]
            max_down_v = max(down_vols) if down_vols else 999999999
            
            is_pocket = c[i] > o[i] and v[i] > max_down_v
            
            # 2. VCP 紧致度 (最近 10 天波幅)
            tightness = np.std((h[i-10:i+1] - l[i-10:i+1]) / l[i-10:i+1] * 100)
            
            if is_pocket and tightness < 1.5:
                tag = "💎口袋起爆" if i == -1 else f"✨{abs(i)-1}日前起爆"
                found_signal = True
                break
        
        # 3. 补位策略：龙回头
        max_gain_20d = (np.max(df['Close'].tail(20)) / np.min(df['Close'].tail(30).head(10)) - 1) * 100
        if not found_signal and max_gain_20d > 28 and abs(price-ma20)/ma20 < 0.04:
            tag = "🐉老龙回头"
            found_signal = True

        # 4. 筹码分布 (POC)
        v_profile, bins = np.histogram(df['Close'].tail(120), bins=50, weights=df['Volume'].tail(120))
        poc = (bins[np.argmax(v_profile)] + bins[np.argmax(v_profile)+1]) / 2
        
        # --- B. 综合评分 (增强适应性) ---
        score = (rs_raw_val * 50) + sector_rank_bonus
        if "起爆" in tag: score += 20
        if "龙" in tag: score += 10
        
        # 广度修正：如果行情极差，降低及格线
        final_score = score * breadth_factor
        
        return tag, poc, round(final_score, 1), found_signal
    except Exception as e:
        return "ERR", 0, 0, False

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v24_pathfinder():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 🚀 A股猎手 V24.0 Pathfinder 启动 (支持周末回溯复盘)...")

    # 1. 大盘广度探测
    try:
        idx = yf.download("000300.SS", period="350d", progress=False)
        idx_c = idx['Close'].iloc[:, 0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close']
    except: return print("❌ 无法获取大盘数据")

    # 2. 获取池子
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "volume", "close", "industry", "change"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 8e9}],
        "range": [0, 600], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "industry": d['d'][5], "chg": d['d'][6], "mkt": d['d'][2]} for d in raw_data])
    except: return print("❌ TV 接口访问失败")

    # 3. 行业与广度计算
    sector_perf = df_pool.groupby('industry')['chg'].mean().sort_values(ascending=False)
    top_sectors = sector_perf.head(max(1, len(sector_perf)//5)).index.tolist()

    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    breadth_check = yf.download(tickers[:100], period="250d", progress=False)['Close']
    uptrend_count = sum([1 for t in breadth_check.columns if breadth_check[t].iloc[-1] > breadth_check[t].rolling(200).mean().iloc[-1]])
    
    # 广度因子修正：提升权重影响力
    breadth_factor = max(0.4, min(1.0, (uptrend_count / 100) * 1.5))
    print(f" -> 📊 市场广度: {uptrend_count}% | 修正因子: {round(breadth_factor, 2)}")

    # 4. 核心扫描
    final_list = []
    chunk_size = 60
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 处理进度: {i+1} ~ {min(i+chunk_size, len(tickers))}...")
        data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                df_h = data[t].dropna()
                if len(df_h) < 200: continue
                p = df_h['Close'].iloc[-1]
                
                # RS 计算
                rs_raw = (p / df_h['Close'].iloc[-250]) / (idx_c.iloc[-1] / idx_c.iloc[-250])
                if rs_raw < 1.0: continue # 基准：至少不能比指数差

                c_code = t.split('.')[0]
                row = df_pool[df_pool['code'] == c_code].iloc[0]
                bonus = 15 if row['industry'] in top_sectors else 0
                
                tag, poc, score, is_match = analyze_v24_logic(df_h, rs_raw, bonus, breadth_factor)
                
                # 降低及格线到 45 分，确保“探路者”能发现微弱火种
                if score < 45 and not is_match: continue

                final_list.append({
                    "Ticker": c_code, "Name": row['name'], "综合评分": score, "战术勋章": tag, 
                    "行业": row['industry'], "RS评级": rs_raw, "市值(亿)": round(row['mkt']/1e8, 2), "Price": round(float(p), 2)
                })
            except: continue

    # 5. 诊断输出
    sh = init_sheet()
    sh.clear()
    
    if not final_list:
        diag_msg = f"⚠️ 诊断：市场广度极低({uptrend_count}%)，全场无及格标的。建议空仓休整。"
        sh.update_acell("A1", diag_msg)
        print(diag_msg)
        return

    # 6. 排序与写入
    res_df = pd.DataFrame(final_list)
    res_df['RS评级'] = res_df['RS评级'].rank(pct=True).apply(lambda x: int(x*99))
    res_df = res_df.sort_values(by="综合评分", ascending=False).head(50)

    cols = ["Ticker", "Name", "综合评分", "战术勋章", "行业", "RS评级", "市值(亿)", "Price"]
    sh.update(range_name="A1", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("J1", f"V24.0 Pathfinder | Breadth: {uptrend_count}% | Updated: {now_str}")

    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            # 对 1-2日前起爆的个股加亮，辅助复盘
            rule_history = ConditionalFormatRule(ranges=[GridRange.from_a1_range('D2:D50', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['日前']),
                    format=cellFormat(backgroundColor=color(0.95, 0.95, 0.95), textFormat=textFormat(italic=True))))
            fmt_rules.append(rule_history)
            fmt_rules.save()
        except: pass
    
    print(f"✅ V24.0 任务完成！已成功抓取 {len(res_df)} 只潜力种子。")

if __name__ == "__main__":
    run_v24_pathfinder()
