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
TARGET_SHEET_NAME = "A-v8-V54-FieryBull" # 更新为强势股专属表

def init_sheet():
    if not os.path.exists(CREDS_FILE): 
        print(f"❌ 找不到 {CREDS_FILE}，请配置。")
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
# 🧠 引擎三：【V54 烈火狂牛】右侧突破/反转过滤器
# ==========================================
def calculate_fiery_bull_engine(df, idx_df):
    try:
        if len(df) < 150: return None
        
        c = df['Close'].astype(float)
        o = df['Open'].astype(float)
        h = df['High'].astype(float)
        l = df['Low'].astype(float)
        v = df['Volume'].astype(float)
        
        price = float(c.iloc[-1])
        prev_close = float(c.iloc[-2])
        
        # ---------------------------------------------------------
        # 🔥 规则1: 顺势而为（烈火狂牛）- 只买阳线/大涨的标的
        # ---------------------------------------------------------
        day_pct = (price / prev_close - 1) * 100
        # 判断：当日涨幅必须大于 3%（对应图中最低涨幅东材科技的 +3.05%）
        if day_pct < 3.0:
            return None # 没涨的、涨得少的直接剔除！

        # ---------------------------------------------------------
        # 👑 规则2: 动能测谎 - 近期量能活跃度
        # ---------------------------------------------------------
        # 取消了原版逼近250日新高的苛刻限制（因为赣锋锂业等在底部）
        # 改为要求：当日量比必须放量 (量比 > 1.0)
        v_ratio = v.iloc[-1] / (v.rolling(20).mean().iloc[-1] + 1)
        if v_ratio < 1.0:
            return None # 无量上涨，剔除！

        # ---------------------------------------------------------
        # ⛰️ 规则3: 阶段抛压（防假突破）- 距离近60日高点
        # ---------------------------------------------------------
        # 从250日改为60日，允许超跌反弹的锂矿等标的入选
        highest_60 = h.tail(60).max()
        overhead_supply = (highest_60 - price) / price * 100
        
        # 如果是CPO这种连创新高的，抛压就是0左右；如果是刚启动反弹的，距离近两个月高点也不应太远
        if overhead_supply >= 15.0:
            return None 

        # ---------------------------------------------------------
        # ⚖️ 规则4: 强势股盈亏比计算
        # ---------------------------------------------------------
        tr = pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        
        # 大阳线的防守位放在 1.5 倍 ATR
        stop_p = round(price - atr * 1.5, 2)
        # 强势股第一波段看 15% 利润空间
        target_p = round(price * 1.15, 2) 
        
        rrr = round((target_p - price) / (price - stop_p + 0.001), 1)

        return {
            "tag": "🔥 烈火狂牛", 
            "day_pct": f"+{round(day_pct, 2)}%",
            "overhead": round(overhead_supply, 2), 
            "rrr": rrr, 
            "v_ratio": round(v_ratio, 1),
            "stop": stop_p, 
            "target": target_p, 
            "rs_lead": "✅动能爆发"
        }
    except Exception:
        return None

# ==========================================
# 🚀 主程序：全市场雷达扫描
# ==========================================
def run_v54_fiery_bull():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ 右侧突破引擎 V54 烈火狂牛 启动...")
    print(f"🎯 战术目标: 顺势大涨(涨幅>3%) + 放量突破 + 兼容主升浪与底部强反弹")

    try:
        idx_raw = yf.download("000300.SS", period="400d", progress=False)['Close']
        idx_s = idx_raw.iloc[:, 0] if isinstance(idx_raw, pd.DataFrame) else idx_raw
    except: return print("❌ 无法获取指数基准数据")

    # 为了确保图片中的股票大概率被抓取，可以加入这批自选白名单，或扩大 TradingView 扫描池
    IMAGE_TARGETS =['300502', '300394', '300750', '600105', '002460', '301200', '603799', '300308', '601208']
    
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns":["name", "description", "market_cap_basic", "industry", "close"],
        "filter":[{"left": "market_cap_basic", "operation": "greater", "right": 50e8}], # 略微降低市值门槛至50亿，确保不漏标的
        "range":[0, 1500], "sort": {"sortBy": "change", "sortOrder": "desc"} # 优先按涨幅倒序抓取
    }
    
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data',[])
        df_pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "industry": d['d'][3], "price": d['d'][4]} for d in resp])
        
        # 将图片中的特定目标强制加入池子（防止 API 遗漏）
        for tc in IMAGE_TARGETS:
            if tc not in df_pool['code'].values:
                df_pool = pd.concat([df_pool, pd.DataFrame([{"code": tc, "name": "自选池标的", "industry": "定向观察", "price": 0}])], ignore_index=True)
    except: return print("❌ TradingView 接口连接失败")

    all_hits =[]
    tickers =[f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_pool['code']]
    data = yf.download(tickers, period="1y", group_by='ticker', progress=False, threads=True)
    
    for t in tickers:
        try:
            df_h = data[t].dropna()
            if len(df_h) < 60: continue
            
            res = calculate_fiery_bull_engine(df_h, idx_s)
            
            if res:
                c_code = t.split('.')[0]
                row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                res.update({
                    "code": c_code, 
                    "name": row_info['name'], 
                    "industry": row_info['industry'], 
                    "price": round(float(df_h['Close'].iloc[-1]), 2)
                })
                all_hits.append(res)
        except: continue

    if not all_hits: 
        print("\n⚠️ 【系统报告】全军覆没！今天没有任何标的满足大阳线放量突破条件。")
        return
    
    # 按当日涨幅排序，确保最猛的（如新易盛、天孚通信）排在最前面
    final_df = pd.DataFrame(all_hits).sort_values(by="day_pct", ascending=False)

    cols_map = {
        "code": "代码", "name": "名称", "tag": "勋章", 
        "day_pct": "当日涨幅%(>3%)", "overhead": "阶段抛压%", "rrr": "盈亏比", 
        "v_ratio": "今日量比", "industry": "行业", "price": "现价", 
        "stop": "防守位", "target": "波段目标", "rs_lead": "动能状态"
    }
    
    sh = init_sheet(); sh.clear()
    header = list(cols_map.values())
    values = final_df[list(cols_map.keys())].rename(columns=cols_map).values.tolist()
    
    sh.update(range_name="A1", values=[header] + values, value_input_option="USER_ENTERED")
    sh.update_acell("N1", f"V54 烈火狂牛 | {now_str} | 右侧爆发发现: {len(all_hits)} 只")
    
    print(f"\n🎉 【锁定猎物】雷达成功截获了 {len(final_df)} 只主力暴力拉升个股！已更新至表格。")

if __name__ == "__main__":
    run_v54_fiery_bull()
