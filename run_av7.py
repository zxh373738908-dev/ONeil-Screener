import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
import yfinance as yf

# 环境屏蔽
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
        print(f"❌ 授权失败: {e}"); exit(1)

# ==========================================
# 🧠 2. V53.0 至臻质量引擎
# ==========================================
def calculate_apex_quality_engine(df, idx_df):
    try:
        if len(df) < 250: return None
        c = df['Close'].astype(float); h = df['High'].astype(float)
        l = df['Low'].astype(float); v = df['Volume'].astype(float)
        price = float(c.iloc[-1])
        
        # 1. 基础门槛：Stage 2 趋势 & 活跃度
        if (c * v).tail(20).mean() < 1.2e8: return None # 日均成交额 > 1.2亿
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        if price < ma50 or ma50 < ma200: return None

        # 2. 相对强度 (RS) 系统
        # 核心：计算 1/3/6/12 个月加权相对强度
        rs_raw = ( (price/c.iloc[-21])*0.4 + (price/c.iloc[-63])*0.2 + (price/c.iloc[-126])*0.2 + (price/c.iloc[-252])*0.2 )
        rs_line = c / idx_df
        # 检查 RS 线是否创 250 日新高 (米勒维尼蓝色点)
        rs_max_250 = rs_line.tail(250).max()
        is_rs_lead = rs_line.iloc[-1] >= rs_max_250 * 0.98

        # 3. 波动收缩 (VCP) & 紧致度
        # 计算过去 10 天的振幅
        tightness = (h.tail(10).max() - l.tail(10).min()) / l.tail(10).min() * 100
        # 成交量枯竭 (VDU)：最近 5 天是否有极致缩量
        vdu_signal = (v.tail(5) < v.rolling(60).mean().tail(5) * 0.55).any()
        
        # 4. 吸筹质量 (Accumulation/Distribution)
        # 计算上涨日成交量与下跌日成交量的比值
        up_vol = v.tail(20)[c.diff().tail(20) > 0].sum()
        dn_vol = v.tail(20)[c.diff().tail(20) < 0].sum()
        ad_ratio = up_vol / (dn_vol + 1)

        # 5. 乖离率 (Anti-FOMO)
        bias_50 = (price / ma50 - 1) * 100
        is_extended = bias_50 > 22 # 离 50 日均线太远则标记为过热

        # 6. 模态勋章
        tag = "关注"
        if is_rs_lead and tightness < 4 and vdu_signal: tag = "💎 巅峰奇点"
        elif is_rs_lead and price > h.tail(250).max() * 0.95: tag = "🥇 领头羊突破"
        elif ad_ratio > 2.0 and tightness < 5: tag = "🔋 机构高位吸筹"
        elif tightness < 2.5: tag = "🌪️ 极致紧致"
        
        # 过滤：如果不是领头羊也不是 VCP，且今日收跌幅度大，则剔除
        if tag == "关注" or price < c.iloc[-2] * 0.96: return None 

        # 7. 止损与盈亏比
        atr = (pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)).rolling(14).mean().iloc[-1]
        stop_p = round(price - (atr * 1.5), 2)
        target_p = round(h.tail(250).max() * 1.15, 2)
        rrr = round((target_p - price) / (price - stop_p + 0.01), 1)

        # 综合评分 (Quality Score)
        quality_score = (rs_raw * 50) + (ad_ratio * 15) + (max(0, 5-tightness)*10)

        return {
            "tag": tag, "score": quality_score, "rs_idx": rs_raw, 
            "rs_lead": "✅" if is_rs_lead else "❌", "bias": round(bias_50, 1), 
            "ad": round(ad_ratio, 1), "tight": round(tightness, 1),
            "rrr": rrr, "stop": stop_p, "target": target_p, "is_ext": is_extended
        }
    except: return None

# ==========================================
# 🚀 3. 主程序逻辑
# ==========================================
def run_v53_apex():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ V53.0 Apex Quality 启动...")

    # 1. 抓取沪深300基准
    idx_raw = yf.download("000300.SS", period="400d", progress=False)['Close']
    idx_s = idx_raw.iloc[:, 0] if isinstance(idx_raw, pd.DataFrame) else idx_raw
    
    # 2. 从 TradingView 获取基础池 (市值 > 85亿)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "industry", "close"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 85e8}],
               "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])
    except: return print("❌ 数据接口故障")

    # 3. 批量执行引擎
    all_hits = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    for t in tickers:
        try:
            df_h = data[t].dropna()
            if len(df_h) < 150: continue
            c_code = t.split('.')[0]; row_info = df_pool[df_pool['code'] == c_code].iloc[0]
            res = calculate_apex_quality_engine(df_h, idx_s)
            if res:
                res.update({"code": c_code, "name": row_info['name'], "industry": row_info['industry'], "price": row_info['price']})
                all_hits.append(res)
        except: continue

    if not all_hits: return print("⚠️ 市场暂无至臻质量信号。")
    
    # 4. 横向 RS 排名 & 行业主线
    final_raw_df = pd.DataFrame(all_hits)
    final_raw_df['RS评级'] = final_raw_df['rs_idx'].rank(pct=True).apply(lambda x: int(x*99))
    
    industry_rank = final_raw_df.groupby("industry")['RS评级'].transform('mean')
    final_raw_df['板块地位'] = industry_rank.apply(lambda x: "🔥 领涨主线" if x > 85 else "普通")
    final_raw_df['勋章'] = final_raw_df.apply(lambda r: "⚠️ 极致乖离" if r['is_ext'] else r['tag'], axis=1)

    # 5. 排序与精选 (RS评级 > 75 且 行业去重)
    final_df = final_raw_df[final_raw_df['RS评级'] > 75].sort_values(by="score", ascending=False)
    final_df = final_df.groupby("industry").head(4).head(60)

    # 6. 写入 Google Sheets
    cols_map = {
        "code": "代码", "name": "名称", "勋章": "勋章", "RS评级": "RS评级", "rs_lead": "RS线新高",
        "ad": "吸筹比", "tight": "紧致度", "bias": "50日乖离", "rrr": "盈亏比", 
        "板块地位": "板块地位", "industry": "行业", "price": "现价", "stop": "止损", "target": "目标"
    }
    
    sh = init_sheet(); sh.clear()
    sh.update(range_name="A1", values=[list(cols_map.values())] + final_df[list(cols_map.keys())].rename(columns=cols_map).values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("P1", f"Apex V53.0 Quality | {now_str} | Breadth: {len(all_hits)}")
    
    print(f"🎉 扫描圆满成功！已筛选 {len(final_df)} 只至臻潜力股。")

if __name__ == "__main__":
    run_v53_apex()
