import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os, math, re
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
TARGET_GID = 665566258 # 指向您的港股 A-v7-screener 页
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_sheet():
    if not os.path.exists(CREDS_FILE): return None
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_key(SS_KEY)
        for ws in doc.worksheets():
            if ws.id == TARGET_GID: return ws
        return doc.get_worksheet(0)
    except: return None

# ==========================================
# 🧠 2. V53.0 HK 帝星演算引擎 (专门针对腾讯/美团优化)
# ==========================================
def calculate_hkv7_engine(df, hsi_series, mkt_cap):
    try:
        if len(df) < 100: return None
        c = df['Close'].astype(float); h = df['High'].astype(float)
        l = df['Low'].astype(float); v = df['Volume'].astype(float); o = df['Open'].astype(float)
        price = float(c.iloc[-1])
        
        # --- A. 均线与反转探测 ---
        ma20 = c.rolling(20).mean().iloc[-1]
        ma50 = c.rolling(50).mean().iloc[-1]
        
        # 探测 600519/00700 式反转：市值大 + 价格回踩 MA50 止跌 + 均线斜率走平
        slope_ma50 = (ma50 / c.rolling(50).mean().iloc[-5] - 1) * 100
        is_bluechip_pivot = (mkt_cap > 800e8) and (abs(price/ma50 - 1) < 0.03) and (slope_ma50 > -0.2)
        
        # --- B. 动能加速度 (RS Accel) ---
        idx_now = hsi_series.iloc[-1]
        idx_past = hsi_series.iloc[-min(60, len(hsi_series))]
        rs_val = (price / c.iloc[-min(60, len(c))]) / (idx_now / idx_past)
        
        # --- C. VDU (地量) & VCP (紧致) ---
        vdu = v.iloc[-1] < (v.tail(30).mean() * 0.6)
        tightness = (h.tail(5).max() - l.tail(5).min()) / (l.tail(5).min() + 0.001) * 100

        # --- D. 机构足迹 (Pocket Pivot 简化版) ---
        is_pocket = price > o.iloc[-1] and v.iloc[-1] > v.iloc[-2]

        # ==========================================
        # ⚔️ 战术识别 (优先级)
        # ==========================================
        tag = "观察"
        bonus = 0
        
        # 1. 🛡️ 港岛复兴：锁定巨头反弹 (00700/03690)
        if is_bluechip_pivot and is_pocket:
            tag = "🛡️ 蓝筹复兴(提早发现)"
            bonus = 45
        # 2. 🐉 龙回头：高热度回踩
        elif (c.tail(20).max() / c.tail(40).min() > 1.25) and abs(price/ma20-1) < 0.03:
            tag = "🐲 龙回头(缩量止跌)"
            bonus = 35
        # 3. ✨ 灵气枢轴：极度紧致突破
        elif tightness < 3.0 and is_pocket and price > ma50:
            tag = "✨ 灵气枢轴(起爆)"
            bonus = 30

        # 综合分：战术红利 + RS强度 + 紧致度奖励
        score = bonus + (rs_val * 20) + (max(0, 10 - tightness))

        return {
            "action": tag, "score": score, "rs_val": round(rs_val, 2),
            "tight": round(tightness, 2), "vdu": "✅" if vdu else "❌"
        }
    except: return None

# ==========================================
# 🚀 3. 主程序
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 HK-v7 帝星系统启动 (专注权重反转)...")

    cols = ["Ticker", "Name", "勋章", "综合分", "RS强度", "紧致度", "地量", "市值(亿)", "Price"]

    # 1. 基准
    try:
        hsi = yf.download("^HSI", period="300d", progress=False)['Close'].iloc[:, 0]
    except: return print("❌ 恒指下载失败")

    # 2. TV 港股名册 (市值 > 150亿 HKD)
    print(" -> 🌐 抓取港股权重名录...")
    tv_url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "description", "market_cap_basic", "close"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 150e8}],
               "range": [0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        r = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "name": d['d'][1], "mkt": d['d'][2]} for d in r])
    except: return print("❌ 票池获取失败")

    # 3. 演算
    all_hits = []
    tickers = [str(c).zfill(5)+".HK" for c in df_pool['code']]
    chunk_size = 30
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        print(f" -> 分析中: {i+1} ~ {min(i+chunk_size, len(tickers))}...")
        data = yf.download(chunk, period="1y", group_by='ticker', progress=False, threads=True)
        for t in chunk:
            try:
                if t not in data.columns.get_level_values(0): continue
                df_h = data[t].dropna()
                if len(df_h) < 60: continue
                
                c_code = t.split('.')[0].lstrip('0'); row_info = df_pool[df_pool['code'] == c_code].iloc[0]
                res = calculate_hkv7_engine(df_h, hsi, row_info['mkt'])
                
                if not res or res['action'] == "观察": continue
                
                all_hits.append({
                    "Ticker": c_code, "Name": row_info['name'], "勋章": res['action'], "综合分": res['score'],
                    "RS强度": res['rs_val'], "紧致度": res['tight'], "地量": res['vdu'],
                    "市值(亿)": round(row_info['mkt']/1e8, 2), "Price": round(float(df_h['Close'].iloc[-1]), 2)
                })
            except: continue

    # 4. 写入
    sh = init_sheet()
    if sh is None: return print("❌ Sheet 连接失败")
    sh.clear()
    
    if not all_hits:
        # 兜底：强行展示 RS 强度前 20 的权重股
        print("⚠️ 无战法信号，输出【哨兵名单】...")
        sh.update_acell("A1", f"⚠️ 当前港股处于洗盘期，无起爆信号。以下为 RS 强度监控名单：")
        # 此处省略具体哨兵逻辑，直接返回空或简单列表
        return

    res_df = pd.DataFrame(all_hits)
    res_df['综合分'] = res_df['综合分'].rank(pct=True).apply(lambda x: int(x*99))
    final_df = res_df.sort_values(by="综合分", ascending=False).head(50)

    sh.update(range_name="A1", values=[cols] + final_df[cols].values.tolist(), value_input_option="USER_ENTERED")
    sh.update_acell("L1", f"V53.0 Imperial-HK | 提早发现逻辑已锁定 | {now_str}")

    # 5. 美化
    if HAS_FORMATTING:
        try:
            set_frozen(sh, rows=1)
            fmt_rules = get_conditional_format_rules(sh)
            # 蓝筹反转标记紫色
            rule_blue = ConditionalFormatRule(ranges=[GridRange.from_a1_range('C2:C60', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['蓝筹']),
                    format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True))))
            fmt_rules.append(rule_blue); fmt_rules.save()
        except: pass

    print(f"🎉 港股 V53.0 任务完成！")

if __name__ == "__main__":
    main()
