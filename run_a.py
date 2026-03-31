import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os, math
import yfinance as yf

# --- [修复核心] 尝试导入美化插件 ---
try:
    from gspread_formatting import *
    HAS_FORMATTING = True
except ImportError:
    HAS_FORMATTING = False

# 基础干扰屏蔽
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 配置中心
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))
ACCOUNT_SIZE = 100000 

def init_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    try:
        # 统一使用 V47 表名
        return doc.worksheet("A-Share V47-Supreme")
    except:
        return doc.add_worksheet(title="A-Share V47-Supreme", rows=1000, cols=20)

# ==========================================
# 🧠 2. V47.1 巅峰统领引擎 (针对白马反转优化)
# ==========================================
def calculate_supreme_engine(df, index_series, mkt_cap):
    try:
        if len(df) < 150: return None
        # 强制降维防止 Series 歧义
        c = df['Close'].astype(float)
        h = df['High'].astype(float)
        l = df['Low'].astype(float)
        v = df['Volume'].astype(float)
        o = df['Open'].astype(float)
        price = float(c.iloc[-1])
        
        # --- A. 均线与趋势模板 ---
        ma20 = c.rolling(20).mean().iloc[-1]
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        is_stage_2 = price > ma50 > ma200
        
        # --- B. 相对强度 (IBD加权模式) ---
        # 计算 120 日相对大盘强度
        rs_val = (price / c.iloc[-min(120, len(c))]) / (index_series.iloc[-1] / index_series.iloc[-min(120, len(index_series))])
        
        # --- C. 机构吸筹 (OBV) 与 紧致度 ---
        obv = (np.sign(c.diff()) * v).fillna(0).cumsum()
        is_obv_up = obv.iloc[-1] > obv.tail(10).mean()
        tightness = (c.tail(10).std() / c.tail(10).mean()) * 100
        
        # --- D. 涨停/脉冲基因 ---
        has_limit_up = any(c.tail(10).pct_change() > 0.09)
        
        # --- E. 止损位计算 (ADR动态) ---
        adr = ((h - l) / l).tail(20).mean()
        stop_p = price * (1 - adr * 1.8)

        # ==========================================
        # ⚔️ 指令决策树 (600519 提前选出逻辑)
        # ==========================================
        action = "观察"
        # 1. 🐉 黄金支点 (专抓茅台反转)
        # 逻辑：大市值 + 站稳MA20 + 资金流(OBV)先行
        if mkt_cap > 1000e8 and price > ma20 and is_obv_up:
            action = "🐉 黄金支点(白马归来)"
        # 2. 🚀 龙抬头 (题材强力回踩)
        elif has_limit_up and tightness < 2.5 and v.iloc[-1] < v.tail(5).mean():
            action = "🐲 龙回头(V-Dry)"
        # 3. ✨ 动量突破
        elif price > ma50 and price >= c.tail(120).max() * 0.98:
            action = "🚀 动量突破(Breakout)"

        return {
            "action": action, "rs_raw": rs_val, "tight": round(tightness, 2), 
            "obv": "✅" if is_obv_up else "❌", "adr": round(adr*100, 2), 
            "stop": round(stop_p, 2), "limit": "⚡" if has_limit_up else "-"
        }
    except: return None

# ==========================================
# 🚀 3. 主扫描流程
# ==========================================
def run_v47_supreme():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 A股 V47.1 统领指挥部：正在全域扫描...")

    # 1. 获取基准 (沪深300)
    idx_raw = yf.download("000300.SS", period="300d", progress=False)
    idx_close = idx_raw['Close'].iloc[:, 0] if isinstance(idx_raw['Close'], pd.DataFrame) else idx_raw['Close']
    
    # 2. TV 云端筛选 (市值 > 65 亿)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "volume", "industry", "close", "change"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 65e8}],
        "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([
            {"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][4], "price": d['d'][5], "chg": d['d'][6]} 
            for d in resp
        ])
    except: return print("❌ TV 接口响应异常")

    # 3. 执行演算
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    chunk_size = 40
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                if t not in data.columns.get_level_values(0): continue
                df_h = data[t].dropna()
                if df_h.empty: continue
                
                c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                res = calculate_supreme_engine(df_h, idx_close, row_info['mkt'])
                
                if not res or res['action'] == "观察": continue

                all_hits.append({
                    "Ticker": c_code, "Name": row_info['name'], "指令": res['action'], 
                    "RS_Raw": res['rs_raw'], "行业": row_info['industry'], "板块热力": 0,
                    "OBV支撑": res['obv'], "涨停基因": res['limit'], "紧致度": res['tight'],
                    "ADR%": res['adr'], "止损价": res['stop'], "现价": row_info['price']
                })
            except: continue

    if not all_hits: return print("⚠️ 全场无共振信号")

    # 4. 🔥 集群加权逻辑
    res_df = pd.DataFrame(all_hits)
    industry_counts = res_df['行业'].value_counts()
    
    def supreme_boost(row):
        count = industry_counts[row['行业']]
        if count >= 3:
            row['指令'] = f"👑 统领 | {row['指令']}"
            row['RS_Raw'] *= 1.1 # 板块加成
        row['板块热力'] = count
        return row

    res_df = res_df.apply(supreme_boost, axis=1)
    
    # 5. 排序与写入
    res_df['RS评级'] = res_df['RS_Raw'].rank(pct=True).apply(lambda x: int(x*99))
    res_df = res_df.sort_values(by="RS评级", ascending=False).head(60)

    sh = init_sheet(); sh.clear()
    cols = ["Ticker", "Name", "指令", "RS评级", "板块热力", "行业", "OBV支撑", "涨停基因", "紧致度", "ADR%", "止损价", "现价"]
    # 确保列对齐
    final_data = res_df[cols].fillna("-")
    sh.update(range_name="A1", values=[cols] + final_data.values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("M1", f"V47.1 Supreme | RESILIENT MODE | {now_str}")

    # 6. 视觉美化 (安全调用修复)
    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            rule_cmd = ConditionalFormatRule(
                ranges=[GridRange.from_a1_range('C2:C65', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['👑']),
                    format=cellFormat(backgroundColor=color(1, 0.9, 0.6), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0.2, 0))))
            )
            fmt_rules.append(rule_cmd); fmt_rules.save()
        except Exception as e:
            print(f"⚠️ 美化插件应用失败: {e}")

    print(f"🎉 V47.1 统领任务圆满完成！数据已同步。")

if __name__ == "__main__":
    run_v47_supreme()
