import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests, os
import yfinance as yf

# ==========================================
# 🛡️ 环境屏蔽与配置中心
# ==========================================
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird" # 泣血早鸟专属表

def init_sheet():
    """初始化 Google Sheets 链接"""
    if not os.path.exists(CREDS_FILE): 
        print(f"❌ 找不到 {CREDS_FILE}，请配置。")
        exit(1)
        
    scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_key(SS_KEY)
        if TARGET_SHEET_NAME not in [w.title for w in doc.worksheets()]:
            return doc.add_worksheet(TARGET_SHEET_NAME, 1000, 20)
        return doc.worksheet(TARGET_SHEET_NAME)
    except Exception as e: 
        print(f"❌ Google Sheets 授权失败: {e}")
        exit(1)

# ==========================================
# 🧠 引擎二：【V53.3 泣血早鸟】核心过滤器
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
        
        # ---------------------------------------------------------
        # 🔴 规则1: 逆向红绿灯（鲜血掩护）- 只买绿盘或阴线
        # ---------------------------------------------------------
        day_pct = (price / prev_close - 1) * 100
        # 判断：1D% < 0 (今天跌了) 或 收盘价 < 开盘价 (实体为阴线)
        is_yin_candle = (day_pct < 0) or (price < float(o.iloc[-1]))
        if not is_yin_candle:
            return None # 只要是阳线/大涨，直接剔除！绝不追高！

        # ---------------------------------------------------------
        # 👑 规则2: 底牌测谎（战术灵魂）- RS线新高
        # ---------------------------------------------------------
        rs_line = c / idx_df
        rs_max_250 = rs_line.tail(250).max()
        # 股票虽然今天跌了，但相对大盘的RS强度极其强悍，无限逼近甚至超越一年新高！(容错率1%)
        is_rs_lead = (rs_line.iloc[-1] >= rs_max_250 * 0.99)
        if not is_rs_lead:
            return None # 灵魂不存在，主力没在护盘，剔除！

        # ---------------------------------------------------------
        # ⛰️ 规则3: 上方无泰山（防被埋）- 上方抛压% < 5%
        # ---------------------------------------------------------
        highest_250 = h.tail(250).max()
        overhead_supply = (highest_250 - price) / price * 100
        if overhead_supply >= 5.0:
            return None # 距离新高太远，上方有历史套牢盘，剔除！

        # ---------------------------------------------------------
        # ⚖️ 规则4: 极高赔率（以小博大）- 盈亏比 > 2.0
        # ---------------------------------------------------------
        # 动态ATR止损：由于今天是阴线砸盘，往往已靠近极限支撑，止损设在 1倍ATR
        tr = pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        
        stop_p = round(price - atr * 1.0, 2)
        target_p = round(highest_250 * 1.05, 2) # 前方无套牢盘，目标直接看突破前高5%
        
        rrr = round((target_p - price) / (price - stop_p + 0.001), 1)
        if rrr <= 2.0:
            return None # 跌得不够深，盈亏比算不过来账，剔除！

        # ==========================================
        # 🏆 幸存者：成功扛过极限绞肉机
        # ==========================================
        v_ratio = v.iloc[-1] / (v.rolling(20).mean().iloc[-1] + 1)
        
        return {
            "tag": "🩸 泣血早鸟", 
            "day_pct": round(day_pct, 2),
            "overhead": round(overhead_supply, 2), 
            "rrr": rrr, 
            "v_ratio": round(v_ratio, 1),
            "stop": stop_p, 
            "target": target_p, 
            "rs_lead": "✅量价背离"
        }
    except Exception:
        return None

# ==========================================
# 🚀 主程序：全市场雷达扫描
# ==========================================
def run_v53_blood_bird():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ 极限防御引擎 V53.3 泣血早鸟 启动...")
    print(f"🎯 战术目标: 鲜血掩护(买阴) + RS底牌新高 + 上方无泰山(<5%) + 极高盈亏比(>2)")

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
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "industry": d['d'][3], "price": d['d'][4]} for d in resp])
    except: return print("❌ TradingView 接口连接失败")

    # 3. 核心算法绞肉机
    all_hits =[]
    tickers =[f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    for t in tickers:
        try:
            df_h = data[t].dropna()
            if len(df_h) < 150: continue
            
            res = calculate_blood_bird_engine(df_h, idx_s)
            
            if res:
                c_code = t.split('.')[0]
                row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                res.update({
                    "code": c_code, 
                    "name": row_info['name'], 
                    "industry": row_info['industry'], 
                    "price": row_info['price']
                })
                all_hits.append(res)
        except: continue

    if not all_hits: 
        print("\n⚠️ 【系统报告】全军覆没！\n今天没有主力砸盘诱空的标的，所有股票均不满足四大极限参数。\n💡 架构师建议：管住手，喝杯咖啡，耐心等待大盘暴跌血案日的错杀猎物！")
        return
    
    # 4. 数据输出准备 (盈亏比从高到低排序，寻找最暴利的猎物)
    final_df = pd.DataFrame(all_hits).sort_values(by="rrr", ascending=False)

    cols_map = {
        "code": "代码", "name": "名称", "tag": "勋章", 
        "day_pct": "当日涨跌%(阴线)", "overhead": "抛压%(<5)", "rrr": "盈亏比(>2)", 
        "v_ratio": "今日量比", "industry": "行业", "price": "现价", 
        "stop": "极限止损", "target": "反包目标", "rs_lead": "RS底牌"
    }
    
    sh = init_sheet(); sh.clear()
    header = list(cols_map.values())
    values = final_df[list(cols_map.keys())].rename(columns=cols_map).values.tolist()
    
    sh.update(range_name="A1", values=[header] + values, value_input_option="USER_ENTERED")
    sh.update_acell("N1", f"V53.3 泣血早鸟 | {now_str} | 错杀发现: {len(all_hits)} 只")
    
    print(f"\n🎉 【锁定猎物】极度血腥的行情中，雷达截获了 {len(final_df)} 只主力诱空个股！已更新至表格。")

if __name__ == "__main__":
    run_v53_blood_bird()
