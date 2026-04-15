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
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird" # 升级Sheet表名

def init_sheet():
    """初始化 Google Sheets 链接"""
    if not os.path.exists(CREDS_FILE): 
        print(f"❌ 错误: 找不到 {CREDS_FILE}。请确保已在 GitHub Secrets 中配置。")
        exit(1)
        
    scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_key(SS_KEY)
        if TARGET_SHEET_NAME not in[w.title for w in doc.worksheets()]:
            return doc.add_worksheet(TARGET_SHEET_NAME, 1000, 20)
        return doc.worksheet(TARGET_SHEET_NAME)
    except Exception as e: 
        print(f"❌ Google Sheets 授权失败: {e}")
        exit(1)

# ==========================================
# 🧠 2. V53.3 泣血早鸟 核心演算引擎
# ==========================================
def calculate_blood_bird_engine(df, idx_df):
    try:
        if len(df) < 250: return None
        
        c = df['Close'].astype(float)
        o = df['Open'].astype(float)
        h = df['High'].astype(float)
        l = df['Low'].astype(float)
        v = df['Volume'].astype(float)
        
        price = float(c.iloc[-1])
        prev_close = float(c.iloc[-2])
        
        # 1. 基础 Stage 2 趋势检测 (底牌不倒)
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        if ma50 < ma200 * 0.98 or price < ma50 * 0.95: 
            return None # 均线死叉或跌破生命线太多，不看

        # 2. 内生动力：RS 线新高检测 (灵魂参数)
        rs_line = c / idx_df
        rs_max_250 = rs_line.tail(250).max()
        # 股票虽然在跌，但RS线极其抗跌甚至逆势新高
        is_rs_lead = rs_line.iloc[-1] >= rs_max_250 * 0.97
        rs_raw = ( (price/c.iloc[-21])*0.4 + (price/c.iloc[-63])*0.2 + (price/c.iloc[-126])*0.2 + (price/c.iloc[-252])*0.2 )

        # 3. 逆向红绿灯：买阴不买阳 (1D% < 0 或 收阴线)
        day_pct = (price / prev_close - 1) * 100
        # 判断条件：今天收盘价低于昨天，或者今天收盘价低于开盘价（标准的砸盘阴线）
        is_yin_candle = (price < prev_close) or (price < float(o.iloc[-1]))
        if not is_yin_candle:
            return None # ❌ 删掉旧代码情绪高潮，今天涨停或大阳线一律不买！

        # 4. 抛压测谎：上方抛压% < 5%
        highest_250 = h.tail(250).max()
        overhead_supply = (highest_250 - price) / price * 100

        # 5. 极高赔率测算 (盈亏比 > 2.0)
        # 向下的止损：设定在近期低点或1.2倍ATR (寻找跌到支撑位的票)
        tr = pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        
        # 止损位：近期低点与支撑位结合，极其紧凑
        stop_p = round(price - atr * 1.2, 2)
        
        # 目标位：因为抛压极小，目标直接看突破前高并延伸10%
        target_p = round(highest_250 * 1.05 if overhead_supply < 5 else highest_250, 2)
        
        # 盈亏比 = (预期收益) / (承担风险)
        rrr = round((target_p - price) / (price - stop_p + 0.001), 1)

        # 6. 核心勋章判定 (V53.3 核心)
        tag = "关注"
        
        if is_rs_lead and overhead_supply < 5.0 and rrr > 2.0:
            tag = "🩸 泣血早鸟"
        elif is_rs_lead and overhead_supply < 8.0 and rrr > 1.5:
            tag = "🦅 错杀潜伏"
        else:
            return None # 不符合极高赔率或抛压条件，直接抛弃

        v_ratio = v.iloc[-1] / (v.rolling(20).mean().iloc[-1] + 1)
        
        return {
            "tag": tag, 
            "rs_raw": rs_raw, 
            "day_pct": round(day_pct, 2),
            "overhead": round(overhead_supply, 2), 
            "rrr": rrr, 
            "v_ratio": round(v_ratio, 1),
            "stop": stop_p, 
            "target": target_p, 
            "rs_lead": "✅" if is_rs_lead else "❌"
        }
    except Exception:
        return None

# ==========================================
# 🚀 3. 主程序扫描流程
# ==========================================
def run_v53_blood_bird():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ V53.3 泣血早鸟 启动[错杀/极高赔率提取]...")

    # 1. 抓取基准 (沪深300)
    try:
        idx_raw = yf.download("000300.SS", period="400d", progress=False)['Close']
        idx_s = idx_raw.iloc[:, 0] if isinstance(idx_raw, pd.DataFrame) else idx_raw
    except: return print("❌ 无法获取指数基准数据")

    # 2. 从 TradingView 获取基础池 (市值 > 80亿)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns":["name", "description", "market_cap_basic", "industry", "close"],
        "filter":[{"left": "market_cap_basic", "operation": "greater", "right": 80e8}],
        "range":[0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])
    except: return print("❌ TradingView 接口连接失败")

    # 3. 执行核心算法
    all_hits = []
    tickers =[f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    for t in tickers:
        try:
            df_h = data[t].dropna()
            if len(df_h) < 150: continue
            
            c_code = t.split('.')[0]
            row_info = df_pool[df_pool['code'] == c_code].iloc[0]
            res = calculate_blood_bird_engine(df_h, idx_s)
            
            if res:
                res.update({
                    "code": c_code, 
                    "name": row_info['name'], 
                    "industry": row_info['industry'], 
                    "price": row_info['price']
                })
                all_hits.append(res)
        except: continue

    if not all_hits: return print("⚠️ 今天没有主力砸盘诱空的标的，休息！")
    
    # 4. 横向 RS 排名与综合评分
    final_raw_df = pd.DataFrame(all_hits)
    final_raw_df['RS评级'] = final_raw_df['rs_raw'].rank(pct=True).apply(lambda x: int(x*99))
    
    # 核心排序：重金赋予【泣血早鸟】最高权重，且盈亏比越大越靠前！
    final_raw_df['sort_score'] = final_raw_df['RS评级'] + (final_raw_df['tag'] == "🩸 泣血早鸟").astype(int) * 50 + final_raw_df['rrr'] * 10
    
    # 5. 行业去重并精选前60名
    final_df = (final_raw_df.sort_values(by="sort_score", ascending=False)
                .groupby("industry").head(5).head(60))

    # 6. 更新到 Google Sheets
    cols_map = {
        "code": "代码", "name": "名称", "tag": "勋章", "RS评级": "RS评级", 
        "day_pct": "当日涨跌%", "overhead": "上方抛压%", "rrr": "盈亏比", "v_ratio": "量比",
        "industry": "行业", "price": "现价", "stop": "止损", "target": "目标", "rs_lead": "RS线新高"
    }
    
    sh = init_sheet(); sh.clear()
    header = list(cols_map.values())
    values = final_df[list(cols_map.keys())].rename(columns=cols_map).values.tolist()
    
    sh.update(range_name="A1", values=[header] + values, value_input_option="USER_ENTERED")
    sh.update_acell("N1", f"V53.3 泣血早鸟 | {now_str} | 错杀发现: {len(all_hits)}")
    
    print(f"🎉 任务成功！已截获 {len(final_df)} 只高盈亏比诱空个股到表格。")

if __name__ == "__main__":
    run_v53_blood_bird()
