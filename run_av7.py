import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
import yfinance as yf

# 环境屏蔽与干扰过滤
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 配置中心
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))
TARGET_SHEET_NAME = "A-v7-screener"

def init_sheet():
    if not os.path.exists(CREDS_FILE): 
        print(f"❌ 错误: 找不到 {CREDS_FILE}"); exit(1)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_key(SS_KEY)
        return doc.worksheet(TARGET_SHEET_NAME) if TARGET_SHEET_NAME in [w.title for w in doc.worksheets()] else doc.add_worksheet(TARGET_SHEET_NAME, 1000, 20)
    except Exception as e: 
        print(f"❌ Google Sheets 授权失败: {e}"); exit(1)

# ==========================================
# 🧠 2. V53.2 Early Bird 核心引擎
# ==========================================
def calculate_early_bird_engine(df, idx_df):
    """
    专注于捕捉趋势早期、波动收缩末期及放量突破点的核心逻辑
    """
    try:
        if len(df) < 250: return None
        c = df['Close'].astype(float); h = df['High'].astype(float)
        l = df['Low'].astype(float); v = df['Volume'].astype(float)
        price = float(c.iloc[-1])
        
        # 1. 均线与基础趋势 (Stage 2 预检)
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        if price < ma50 or ma50 < ma200 * 0.98: return None # 必须是趋势向上或刚转头向上

        # 2. 波动性格分析 (ATR %)
        tr = pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)
        atr_pct = tr.rolling(20).mean().iloc[-1] / price * 100 # 衡量这只股票平时的波动大小
        
        # 3. 乖离率检查 (动态阈值)
        bias_50 = (price / ma50 - 1) * 100
        bias_200 = (price / ma200 - 1) * 100
        # 核心逻辑：波动小的蓝筹股(ATR小)允许的乖离度更低，波动大的票允许更高
        # 动态上限 = 12%基础 + (ATR% * 2.5)
        max_allowed_bias = 12 + (atr_pct * 2.5)

        # 4. 相对强度 (RS) 与 RS 线新高
        rs_line = c / idx_df
        rs_max_250 = rs_line.tail(250).max()
        is_rs_lead = rs_line.iloc[-1] >= rs_max_250 * 0.98
        rs_raw = ( (price/c.iloc[-21])*0.4 + (price/c.iloc[-63])*0.2 + (price/c.iloc[-126])*0.2 + (price/c.iloc[-252])*0.2 )

        # 5. 突破特征与紧致度
        # 20日价格新高突破检测
        is_breakout_20 = price >= h.tail(20).max() * 0.99
        # 10日价格紧致度 (振幅)
        tightness = (h.tail(10).max() - l.tail(10).min()) / l.tail(10).min() * 100
        # 今日量能比
        v_ratio = v.iloc[-1] / (v.rolling(20).mean().iloc[-1] + 1)

        # 6. 勋章认定逻辑 (Early Bird 核心)
        tag = "关注"
        # 模式 A: 底部初启 (针对中煤能源等刚从底部爬起来的票)
        if bias_200 < 25 and is_breakout_20 and v_ratio > 1.2:
            tag = "🟢 底部初启"
        # 模式 B: 巅峰奇点 (高强度 VCP 突破)
        elif tightness < 4.5 and is_rs_lead and v_ratio < 1.1:
            tag = "💎 巅峰奇点"
        # 模式 C: 强力主升
        elif is_rs_lead and bias_50 > 5:
            tag = "🚀 强力主升"
        
        # 极致乖离判断 (应用动态阈值)
        if bias_50 > max_allowed_bias:
            tag = "⚠️ 极致乖离"

        # 过滤掉涨得太离谱或今日大幅回撤的标的
        if tag == "关注" or (tag == "⚠️ 极致乖离" and bias_50 > 35) or price < c.iloc[-2] * 0.96:
            return None

        # 7. 风险收益比 (RRR)
        stop_p = round(price - (tr.rolling(14).mean().iloc[-1] * 1.5), 2)
        target_p = round(h.tail(250).max() * (1.1 if price >= h.tail(250).max() else 1.0), 2)
        rrr = round((target_p - price) / (price - stop_p + 0.01), 1)

        return {
            "tag": tag, "rs_raw": rs_raw, "bias": round(bias_50, 1), 
            "tight": round(tightness, 1), "rrr": rrr, "v_ratio": round(v_ratio, 1),
            "stop": stop_p, "target": target_p, "rs_lead": "✅" if is_rs_lead else "❌"
        }
    except Exception:
        return None

# ==========================================
# 🚀 3. 主流程 Orchestrator
# ==========================================
def run_early_bird_screener():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ V53.2 Early Bird 启动 (启动点增强模式)...")

    # 1. 抓取基准 (沪深300)
    try:
        idx_data = yf.download("000300.SS", period="400d", progress=False)['Close']
        idx_s = idx_data.iloc[:, 0] if isinstance(idx_data, pd.DataFrame) else idx_data
    except: return print("❌ 无法获取大盘基准数据")

    # 2. 获取初选池 (TV 市值 > 80亿)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "industry", "close"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 80e8}],
        "range": [0, 1000], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])
    except: return print("❌ TradingView 接口异常")

    # 3. 批量执行
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    for t in tickers:
        try:
            df_h = data[t].dropna()
            if len(df_h) < 180: continue
            c_code = t.split('.')[0]
            row_info = df_pool[df_pool['code'] == c_code].iloc[0]
            
            res = calculate_early_bird_engine(df_h, idx_s)
            if res:
                res.update({
                    "code": c_code, "name": row_info['name'], 
                    "industry": row_info['industry'], "price": row_info['price']
                })
                all_hits.append(res)
        except: continue

    if not all_hits: return print("⚠️ 市场目前没有符合 Early Bird 的启动信号。")
    
    # 4. 横向 RS 评级与排序
    final_raw_df = pd.DataFrame(all_hits)
    final_raw_df['RS评级'] = final_raw_df['rs_raw'].rank(pct=True).apply(lambda x: int(x*99))
    
    # 核心排序：给“🟢 底部初启”额外的权重，让买点更好的票排在前面
    final_raw_df['sort_score'] = final_raw_df['RS评级'] + (final_raw_df['tag'] == "🟢 底部初启").astype(int) * 30
    
    # 5. 行业去重并精选
    final_df = (final_raw_df.sort_values(by="sort_score", ascending=False)
                .groupby("industry").head(5).head(60))

    # 6. 写入 Google Sheets
    cols_map = {
        "code": "代码", "name": "名称", "tag": "勋章", "RS评级": "RS评级", 
        "v_ratio": "量比", "tight": "紧致度", "bias": "50日乖离", "rrr": "盈亏比", 
        "industry": "行业", "price": "现价", "stop": "止损", "target": "目标", "rs_lead": "RS线新高"
    }
    
    sh = init_sheet(); sh.clear()
    sh.update(range_name="A1", 
              values=[list(cols_map.values())] + final_df[list(cols_map.keys())].rename(columns=cols_map).values.tolist(), 
              value_input_option="USER_ENTERED")
    sh.update_acell("N1", f"Apex V53.2 Early Bird | {now_str} | Hits: {len(all_hits)}")
    
    print(f"🎉 扫描圆满成功！已筛选 {len(final_df)} 只高价值个股。")

if __name__ == "__main__":
    run_early_bird_screener()
