import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests
import yfinance as yf

# ================= 配置区 (不改动原有架构) =================
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

# 核心参数：追求爆发前的“安静”
MIN_RS = 75            # 市场前 25% 的强势股
MAX_TIGHTNESS = 4.5    # 5日价格波动率
MAX_BIAS_MA20 = 8.0    # 距离20日线不要太远
# =========================================================

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client.open_by_key(SS_KEY).worksheet(TARGET_SHEET_NAME)

def get_benchmark():
    return yf.download("000300.SS", period="260d", progress=False)['Close']

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 启动 V53.4 Alpha-Bird 终极版...")

    # 1. 抓取股票池
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "industry", "close", "market_cap_basic"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 85e8}], 
        "range": [0, 800]
    }
    raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    
    tickers, meta = [], {}
    for item in raw_data:
        code = item['s'].split(':')[-1]
        yf_code = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        tickers.append(yf_code)
        meta[yf_code] = {"name": item['d'][1], "industry": item['d'][2], "symbol": code}

    # 2. 批量获取 K 线
    print(f"📥 深度扫描 {len(tickers)} 只标的趋势...")
    all_data = yf.download(tickers, period="260d", group_by='ticker', progress=True, threads=True)
    benchmark = get_benchmark()
    
    # 3. RS 评级计算
    rs_map = {}
    for t in tickers:
        try:
            c = all_data[t]['Close'].dropna()
            if len(c) < 150: continue
            # 权重相对强度得分
            s = (c.iloc[-1]/c.iloc[-22]*0.4) + (c.iloc[-1]/c.iloc[-66]*0.3) + (c.iloc[-1]/c.iloc[-120]*0.3)
            rs_map[t] = s
        except: continue
    
    rs_df = pd.DataFrame(list(rs_map.items()), columns=['code', 'score'])
    rs_df['rank'] = rs_df['score'].rank(pct=True) * 99

    # 4. 逻辑扫描
    results = []
    for t in tickers:
        try:
            df = all_data[t].dropna()
            if len(df) < 50: continue
            
            c = df['Close']
            v = df['Volume']
            curr_p = float(c.iloc[-1])
            
            # --- 均线系统 ---
            ma10 = c.rolling(10).mean().iloc[-1]
            ma20 = c.rolling(20).mean().iloc[-1]
            ma50 = c.rolling(50).mean().iloc[-1]
            
            # --- 核心过滤指标 ---
            # 1. 紧致度 (最近5天)
            tightness = (c.iloc[-5:].std() / c.iloc[-5:].mean()) * 100
            # 2. RS 评级
            this_rs = rs_df[rs_df['code'] == t]['rank'].values[0]
            # 3. 量能枯竭 (今日成交量对比5日均量)
            vdu_ratio = v.iloc[-1] / v.iloc[-6:-1].mean()
            # 4. 趋势排列 (MA10 > MA20 > MA50 为多头排列)
            is_uptrend = ma10 > ma20 > ma50
            # 5. RS线高点对比
            rs_line = c / benchmark.reindex(c.index).ffill()
            is_rs_new_high = "⭐新高" if rs_line.iloc[-1] >= rs_line.iloc[-20:].max() else "--"
            
            # --- 触发逻辑 (早鸟伏击) ---
            # 条件：RS高 + 多头趋势 + 紧致 + 距离20日线近 + (量能枯竭或微增)
            if this_rs > MIN_RS and is_uptrend and tightness < MAX_TIGHTNESS:
                bias_20 = ((curr_p - ma20) / ma20) * 100
                if -2 < bias_20 < MAX_BIAS_MA20:
                    
                    # 动态止损：取MA20或ATR止损的较大者
                    atr = (df['High'] - df['Low']).rolling(14).mean().iloc[-1]
                    stop_l = max(ma20 * 0.98, curr_p - 1.5 * atr)
                    target_l = curr_p + (target_p_diff := (curr_p - stop_l) * 3)

                    results.append({
                        "RS评级": int(this_rs),
                        "代码": meta[t]['symbol'],
                        "名称": meta[t]['name'],
                        "现价": round(curr_p, 2),
                        "量比": round(vdu_ratio, 2),
                        "紧致度": f"{round(tightness, 2)}%",
                        "50日乖离": f"{round(((curr_p-ma50)/ma50)*100, 1)}%",
                        "盈亏比": round((target_l - curr_p)/(curr_p - stop_l), 2),
                        "行业": meta[t]['industry'],
                        "止损": round(stop_l, 2),
                        "目标": round(target_l, 2),
                        "RS线新高": is_rs_new_high,
                        "更新时间": now.strftime('%H:%M')
                    })
        except: continue

    # 5. 写入 Google Sheets (不改变位置)
    sh = init_sheet()
    sh.clear()
    
    if results:
        # 按 RS 评级和量比（越小越好）排序
        df_final = pd.DataFrame(results).sort_values(by=["RS评级", "量比"], ascending=[False, True])
        sh.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        print(f"✅ 扫描完成！锁定 {len(df_final)} 只高胜率早鸟，优先展示缩量蓄势品种。")
    else:
        sh.update_acell("A1", f"最后扫描: {now.strftime('%H:%M')} - 市场强势股均在大幅波动，无蓄势点")
        print("⚠️ 没发现标的：说明当前强势股正在剧烈洗盘或拉升中，不符合伏击原则。")

if __name__ == "__main__":
    run_v53_optimizer()
