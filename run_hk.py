import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import yfinance as yf
import requests
import re
import time
from gspread_formatting import *

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_GID = 665566258  
CREDS_FILE = "credentials.json"
ACCOUNT_SIZE = 500000 # 假设 50 万港币总仓位
MAX_RISK_PER_TRADE = 0.008 # 单笔损失控制在总仓位 0.8%

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    for ws in doc.worksheets():
        if str(ws.id) == str(TARGET_GID): return ws
    return doc.get_worksheet(0)

# ==========================================
# 🧠 2. V750 增强演算引擎 (🚀主升浪 + 🔥底部起爆 版)
# ==========================================
def calculate_advanced_v750(df, hsi_series):
    try:
        df = df.dropna(subset=['Close'])
        if len(df) < 252: return None
        
        close = df['Close'].values.astype(float)
        open_p = df['Open'].values.astype(float)
        high = df['High'].values.astype(float)
        low = df['Low'].values.astype(float)
        vol = df['Volume'].values.astype(float)
        cp = close[-1]
        
        # 1. 趋势模板与生命线
        ma10 = np.mean(close[-10:])
        ma20 = np.mean(close[-20:])
        ma50 = np.mean(close[-50:])
        ma200 = np.mean(close[-200:])
        
        # 宽容的 Stage 2（允许站上50日线即视为短期多头）
        is_stage_2 = (cp > ma50 > ma200) and (ma200 > np.mean(close[-220:-200]))
        
        # 🔥优化点1：加入底部反转特征（针对赣锋锂业、京东等）
        is_bottom_reversal = (cp > ma20) and (cp > ma50) and (ma200 > ma50) and (cp > open_p[-1])

        is_main_uptrend = (
            (cp > ma10) and (cp > ma20) and 
            (ma10 > ma50) and (ma20 > ma50) and 
            (ma20 > np.mean(close[-25:-5])) and 
            (cp >= np.max(close[-60:]) * 0.85)
        )

        # 2. RS 加速度 
        hsi_val = hsi_series.reindex(df.index).ffill().values
        rs_line = close / hsi_val
        rs_velocity = (rs_line[-1] - rs_line[-10]) / rs_line[-10] * 100
        rs_nh = rs_line[-1] >= np.max(rs_line[-252:])

        # 🔥优化点2：VCP 紧致度计算排除今天（防止今天大涨破坏历史判定）
        tightness = (np.std(close[-11:-1]) / np.mean(close[-11:-1])) * 100
        
        # 4. 机构能量 
        avg_vol20 = np.mean(vol[-20:])
        vol_surge = vol[-1] / avg_vol20
        vdu = vol[-1] < avg_vol20 * 0.55 

        # 5. 综合战法判定
        action = "观察"
        prio = 50
        
        # 🔥优化点3：新增底部机构建仓判定（捕捉底部放量大阳线）
        if is_bottom_reversal and vol_surge > 1.5 and cp > close[-2] * 1.03:
            action, prio = "🐉 底部巨龙(Reversal)", 93
        elif rs_nh and cp < np.max(close[-20:]) * 1.02 and tightness < 1.8: # 放宽紧致度
            action, prio = "👁️ 奇點先行(Stealth)", 95
        elif is_stage_2 and vdu and tightness < 1.5:
            action, prio = "🐉 老龍回頭(V-Dry)", 90
        elif rs_nh and cp >= np.max(close[-252:]) and vol_surge > 1.3:
            action, prio = "🚀 巔峰突破(Breakout)", 92
        elif is_stage_2 and rs_nh and rs_velocity > 0:
            action, prio = "💎 雙重共振(Leader)", 88

        if is_main_uptrend:
            if action == "观察":
                action, prio = "🚀 主升浪(Uptrend)", 94
            else:
                action = action.replace(")", " + 🚀主升浪)")
                prio += 10

        # ---- 【☠️ 补丁4：筹码峰 (POC) 计算与一票否决】 ----
        hist_close = close[-126:]
        hist_vol = vol[-126:]
        hist_min, hist_max = np.min(hist_close), np.max(hist_close)
        
        if hist_max > hist_min:
            bins = np.linspace(hist_min, hist_max, 50)
            indices = np.clip(np.digitize(hist_close, bins) - 1, 0, 49)
            vol_bins = np.zeros(50)
            np.add.at(vol_bins, indices, hist_vol)
            poc_idx = np.argmax(vol_bins)
            poc_price = bins[poc_idx]
        else:
            poc_price = hist_close[-1]

        dist_poc = ((cp - poc_price) / poc_price) * 100

        # 🔥优化点4：放宽港股延伸判定，尤其是底部起爆允许较大偏离度
        if action != "观察" and dist_poc > 15 and not is_bottom_reversal:
            action = "☠️ 极度延伸(禁买)"
            prio = 10  

        # ---- 【🔥 补丁5：口袋枢轴 (Pocket Pivot) 暗影雷达】 ----
        avg_vol20_series = pd.Series(vol).rolling(20).mean().values
        pocket_pivot = False
        # 🔥优化点5：包含今天，寻找近3天（含今天）的大量抢筹
        for i in range(-3, 1):
            if vol[i] > avg_vol20_series[i] * 1.5 and close[i] > open_p[i]:
                pocket_pivot = True
                break

        # 6. 多重结构止损
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
        adr_stop = cp * (1 - adr_20 * 0.01 * 1.6)
        
        if "主升浪" in action and "禁买" not in action:
            struct_stop = ma20 * 0.98 
        elif "底部" in action:
            struct_stop = low[-1] * 0.98 # 底部起爆以当天低点做止损
        else:
            struct_stop = ma50 * 0.99
            
        final_stop = max(adr_stop, struct_stop) 

        # 7. 建议仓位
        risk_per_share = cp - final_stop
        suggested_shares = 0
        if risk_per_share > 0:
            suggested_shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // risk_per_share

        return {
            "Action": action, 
            "Base_Score": prio, 
            "Price": cp, 
            "Tight": round(tightness, 2), 
            "Vol_Ratio": round(vol_surge, 2), 
            "ADR": round(adr_20, 2), 
            "Stop": round(final_stop, 2),
            "Shares": int(suggested_shares), 
            "RS_Vel": round(rs_velocity, 2),
            "Dist_POC%": round(dist_poc, 2),
            "PocketPivot": "🔥 发现" if pocket_pivot else "",
            # 🔥优化点6：放开强过滤，站上50日线或产生放量底部特征即允许交易
            "trade_allowed": (cp > ma200) or (is_bottom_reversal and pocket_pivot), 
            "rs_raw": (cp/close[-63]*2 + cp/close[-126] + cp/close[-252])
        }
    except: return None

# ==========================================
# 🚀 3. 执行流程
# ==========================================
def main():
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V45-V750 量子领袖版 (全天候捕捉) 启动...")
    
    hsi_raw = yf.download("^HSI", period="300d", progress=False)['Close']
    hsi_series = hsi_raw.iloc[:,0] if isinstance(hsi_raw, pd.DataFrame) else hsi_raw
    hsi_p, hsi_ma50 = hsi_series.iloc[-1], hsi_series.rolling(50).mean().iloc[-1]
    
    url = "https://scanner.tradingview.com/hongkong/scan"
    # 🔥优化点7：降低市值过滤门槛，抓取前600只（80亿港币起）
    payload = {"columns":["name", "description", "close", "market_cap_basic", "sector"],
               "filter":[{"left": "market_cap_basic", "operation": "greater", "right": 8e9}],
               "range":[0, 600], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        resp = requests.post(url, json=payload, timeout=15).json().get('data',[])
        df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "其他"} for d in resp])
    except: return

    final_list =[]
    tickers =[str(c).zfill(4)+".HK" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    for t in tickers:
        try:
            code_raw = t.split('.')[0].lstrip('0')
            if t not in data.columns.levels[0]: continue
            res = calculate_advanced_v750(data[t], hsi_series)
            # 🔥优化点8：应用新的 trade_allowed 逻辑
            if res and res['trade_allowed'] and res['Action'] != "观察":
                res.update({"Ticker": t.split('.')[0], "Sector": df_pool[df_pool['code']==code_raw].iloc[0]['sector']})
                final_list.append(res)
        except: continue

    if not final_list: return
    res_df = pd.DataFrame(final_list)

    # 🔥优化点9：在量子计分中，重奖当天的爆量特征（奖励异动）
    res_df['Final_Score'] = (
        res_df['Base_Score'] 
        + (res_df['RS_Vel'] * 0.6) 
        + res_df['rs_raw'].rank(pct=True) * 20 
        + (10 / np.maximum(res_df['Tight'], 0.1)) ** 2  
        + (res_df['Vol_Ratio'] * 5) # 新增：爆量加分！
    ).round(2)

    top_picks = res_df.sort_values(by="Final_Score", ascending=False).groupby('Sector').head(4)
    top_picks = top_picks.head(60) 

    # 5. 写入与可视化
    sh = init_sheet()
    sh.clear()
    
    weather = "☀️ 激进" if hsi_p > hsi_ma50 else "❄️ 观望"
    header = [[f"🏰 V45-V750 全天候版 (抓突破 + 抄底起爆)", f"环境: {weather}", f"刷新: {now_str}", "风控: 单笔风险 0.8%"]]
    sh.update(range_name="A1", values=header)
    
    cols =["Ticker", "Action", "Final_Score", "Price", "Shares", "Stop", "Tight", "Vol_Ratio", "RS_Vel", "Dist_POC%", "PocketPivot", "ADR", "Sector"]
    sh.update(range_name="A3", values=[cols] + top_picks[cols].values.tolist(), value_input_option="USER_ENTERED")

    # 美化格式
    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:M3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))
    
    rules = get_conditional_format_rules(sh)
    # 底部巨龙 - 蓝色高亮 (新增)
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['底部']),
                                format=cellFormat(backgroundColor=color(0.8, 0.9, 1), textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0.8))))))
    # 原有高亮规则保持
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['👁️']),
                                format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🚀']),
                                format=cellFormat(backgroundColor=color(1.0, 0.9, 0.8), textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0.2, 0.2))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['☠️']),
                                format=cellFormat(backgroundColor=color(0.85, 0.85, 0.85), textFormat=textFormat(bold=True, strikethrough=True, foregroundColor=color(0.5, 0.5, 0.5))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['0']),
                                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0.5, 0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('K4:K100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🔥']),
                                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.9, 0.1, 0.1))))))
    rules.save()
    print(f"✅ 任务完成。成功捕捉 {len(top_picks)} 只强势股/起爆股。")

if __name__ == "__main__":
    main()
