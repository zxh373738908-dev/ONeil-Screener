import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import yfinance as yf
import requests
import re
from gspread_formatting import *

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_GID = 665566258  
CREDS_FILE = "credentials.json"
ACCOUNT_SIZE = 500000 
MAX_RISK_PER_TRADE = 0.008 

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    for ws in doc.worksheets():
        if str(ws.id) == str(TARGET_GID): return ws
    return doc.get_worksheet(0)

# ==========================================
# 🧠 2. V750 增强演算引擎 (修复仓位Bug + 阶级重排)
# ==========================================
def calculate_advanced_v750(df, hsi_series):
    try:
        df = df.dropna(subset=['Close', 'High', 'Low', 'Volume', 'Open'])
        if len(df) < 252: return None
        
        # --- ⏳ 时空锁 ---
        current_hkt = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
        last_date = df.index[-1].date()
        is_live_session = (last_date == current_hkt.date() and current_hkt.hour < 16)
        
        if is_live_session:
            live_price = float(df['Close'].iloc[-1]) 
            df_setup = df.iloc[:-1]                  
        else:
            live_price = float(df['Close'].iloc[-1])
            df_setup = df                            
            
        if len(df_setup) < 252: return None

        close = df_setup['Close'].values.astype(float)
        open_p = df_setup['Open'].values.astype(float)
        high = df_setup['High'].values.astype(float)
        low = df_setup['Low'].values.astype(float)
        vol = df_setup['Volume'].values.astype(float)
        setup_price = close[-1] 

        if np.max(high[-10:]) <= np.min(low[-10:]) * 1.02: return None

        # 指标计算
        ma5, ma10, ma20, ma50, ma200 = np.mean(close[-5:]), np.mean(close[-10:]), np.mean(close[-20:]), np.mean(close[-50:]), np.mean(close[-200:])
        is_stage_2 = (setup_price > ma50 > ma200) and (ma200 > np.mean(close[-220:-200]))

        hsi_val = hsi_series.reindex(df_setup.index).ffill().values
        rs_line = close / hsi_val
        rs_velocity = (rs_line[-1] - rs_line[-10]) / rs_line[-10] * 100
        rs_nh = rs_line[-1] >= np.max(rs_line[-120:]) 

        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100
        avg_vol20 = np.mean(vol[-20:])
        vol_surge = vol[-1] / (avg_vol20 + 1) 
        vdu = vol[-1] < avg_vol20 * 0.55 
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100

        # 暗影雷达 (PocketPivot)
        avg_vol20_series = pd.Series(vol).rolling(20).mean().values
        pocket_pivot = False
        for i in range(-3, 0):
            if vol[i] > avg_vol20_series[i] * 1.2 and close[i] > open_p[i]:
                day_range = high[i] - low[i]
                if day_range > 0 and (close[i] - low[i]) / day_range > 0.5:
                    pocket_pivot = True
                    break

        is_momentum_attack = ((setup_price > ma5) and (ma5 > ma10) and (ma10 > ma20) and (ma20 > ma50) and 
                              (setup_price >= np.max(close[-60:]) * 0.95) and (rs_velocity > 0) and (adr_20 > 2.5) and (tightness < 6.0))
        is_reversal = (setup_price < ma200) and (setup_price > ma20) and pocket_pivot

        # 🎯 权力阶级重排：VCP(奇点) 重新登基为王！
        action, prio = "观察", 50
        
        if rs_nh and setup_price < np.max(close[-20:]) * 1.02 and tightness < 1.8:
            action, prio = "👁️ 奇點先行(Stealth)", 98  # 王座
        elif rs_nh and setup_price >= np.max(close[-252:]) and vol_surge > 1.3:
            action, prio = "🚀 巔峰突破(Breakout)", 95 # 次席
        elif is_momentum_attack:
            action, prio = "⚔️ 凌厉进攻(Momentum)", 92 # 中坚
        elif is_stage_2 and vdu and tightness < 1.5:
            action, prio = "🐉 老龍回頭(V-Dry)", 90
        elif is_reversal:
            action, prio = "🌊 底部巨龙(Reversal)", 88

        # 高空引力网
        hist_close, hist_vol = close[-126:], vol[-126:]
        hist_min, hist_max = np.min(hist_close), np.max(hist_close)
        if hist_max > hist_min:
            bins = np.linspace(hist_min, hist_max, 50)
            indices = np.clip(np.digitize(hist_close, bins) - 1, 0, 49)
            vol_bins = np.zeros(50)
            np.add.at(vol_bins, indices, hist_vol)
            poc_price = bins[np.argmax(vol_bins)]
        else:
            poc_price = hist_close[-1]

        dist_poc = ((setup_price - poc_price) / poc_price) * 100

        if (action == "观察" or "老龍" in action) and dist_poc > 15:
            action, prio = "☠️ 极度延伸(禁买)", 10 
        elif action != "观察" and dist_poc > 45: 
            action, prio = "☠️ 高空危楼(禁买)", 10

        # 盘面追踪
        today_pct = ((live_price - setup_price) / setup_price) * 100
        if action != "观察" and "禁买" not in action:
            if today_pct > 4.5:
                action += " 🚀(已飞勿追)"
                prio -= 5 
            elif today_pct < -3.0:
                action += " 🩸(破位取消)"
                prio = 10 

        # --- 🛡️ 风控与止损 (修复天量股数 Bug) ---
        adr_stop = setup_price * (1 - adr_20 * 0.01 * 1.6)
        if "进攻" in action or "主升浪" in action or "突破" in action:
            struct_stop = ma20 * 0.98 
        elif "底部巨龙" in action:
            struct_stop = low[-3] * 0.95 
        else:
            struct_stop = ma50 * 0.99
            
        final_stop = max(adr_stop, struct_stop) 
        
        # 核心修复：强制建立最低安全气垫（至少承担 ADR的波幅 或 2.5% 的空间风险）
        actual_risk_dist = live_price - final_stop
        min_allowed_risk = live_price * max(0.025, adr_20 * 0.01) # 拒绝止损空间小于 2.5%
        
        effective_risk = max(actual_risk_dist, min_allowed_risk)

        suggested_shares = 0
        if effective_risk > 0:
            suggested_shares = int((ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // effective_risk)

        return {
            "Action": action, "Base_Score": prio, 
            "Live_Price": round(live_price, 3), "Today_Pct": round(today_pct, 2),     
            "Tight": round(tightness, 2), "Vol_Ratio": round(vol_surge, 2), 
            "RS_Vel": round(rs_velocity, 2), "PocketPivot": "🔥 发现" if pocket_pivot else "",
            "Dist_POC%": round(dist_poc, 2), "ADR": round(adr_20, 2), 
            "Stop": round(final_stop, 3), "Shares": suggested_shares, 
            "rs_raw": (setup_price/close[-63]*2 + setup_price/close[-126] + setup_price/close[-252])
        }
    except Exception as e: return None

# ==========================================
# 🚀 3. 执行流程 
# ==========================================
def main():
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🦅 V45.3 精准狙击版启动...")
    
    hsi_raw = yf.download("^HSI", period="300d", progress=False)['Close']
    hsi_series = hsi_raw.iloc[:,0] if isinstance(hsi_raw, pd.DataFrame) else hsi_raw
    hsi_p, hsi_ma50 = hsi_series.iloc[-1], hsi_series.rolling(50).mean().iloc[-1]
    
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns":["name", "description", "close", "market_cap_basic", "sector"],
               "filter":[{"left": "market_cap_basic", "operation": "greater", "right": 1.2e10}],
               "range":[0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
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
            if res and res['Action'] != "观察":
                res.update({"Ticker": t.split('.')[0], "Sector": df_pool[df_pool['code']==code_raw].iloc[0]['sector']})
                final_list.append(res)
        except: continue

    if not final_list: return
    res_df = pd.DataFrame(final_list)

    res_df['Final_Score'] = (res_df['Base_Score'] * 1.5 + res_df['rs_raw'].rank(pct=True) * 20 + np.maximum(0, (5 - res_df['Tight']) * 4)).round(2)
    res_df = res_df.sort_values(by=["Base_Score", "Final_Score"], ascending=[False, False])
    top_picks = res_df.groupby('Sector').head(5).head(60)

    sh = init_sheet()
    sh.clear()
    
    weather = "☀️ 激进" if hsi_p > hsi_ma50 else "❄️ 观望"
    header = [[f"🏰 V45.3 精准狙击版 (修复仓位Bug + VCP王者归来)", f"环境: {weather}", f"刷新: {now_str}", "风控: 单笔风险 0.8%"]]
    sh.update(range_name="A1", values=header)
    
    # 修复：确保总共 14 列，排位精准对应
    cols =["Ticker", "Action", "Final_Score", "Live_Price", "Today_Pct", "Shares", "Stop", "Tight", "Vol_Ratio", "RS_Vel", "PocketPivot", "Dist_POC%", "ADR", "Sector"]
    sh.update(range_name="A3", values=[cols] + top_picks[cols].values.tolist(), value_input_option="USER_ENTERED")

    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:N3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))
    
    rules = get_conditional_format_rules(sh)
    rules.clear() 
    
    # --- 修复后的精准坐标列对齐 ---
    # Col B: Action, Col E: Today_Pct, Col F: Shares
    # Col K: PocketPivot (11th col), Col L: Dist_POC% (12th col)
    
    # 动态警告标签
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['已飞勿追']), format=cellFormat(backgroundColor=color(1.0, 0.7, 0.7), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0, 0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['破位取消']), format=cellFormat(backgroundColor=color(0.3, 0.3, 0.3), textFormat=textFormat(bold=True, strikethrough=True, foregroundColor=color(0.8, 0.8, 0.8))))))
    
    # 王者与主战法高亮
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['👁️']), format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🚀']), format=cellFormat(backgroundColor=color(1.0, 0.9, 0.8), textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0.2, 0.2))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['⚔️']), format=cellFormat(backgroundColor=color(1.0, 0.84, 0.0), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0.2, 0.0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🌊']), format=cellFormat(backgroundColor=color(0.8, 0.9, 1.0), textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0.5))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['☠️']), format=cellFormat(backgroundColor=color(0.85, 0.85, 0.85), textFormat=textFormat(bold=True, strikethrough=True, foregroundColor=color(0.5, 0.5, 0.5))))))
                                
    # 数据列红绿对齐
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['0']), format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0, 0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_LESS', ['0']), format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0.6, 0))))))
    
    # 修正坐标：Col K 是 PocketPivot
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('K4:K100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['🔥']), format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.9, 0.1, 0.1))))))
                                
    # 修正坐标：Col L 是 Dist_POC%
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('L4:L100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_BETWEEN',['-10', '20']), format=cellFormat(textFormat=textFormat(foregroundColor=color(0, 0.6, 0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('L4:L100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER',['45']), format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.9, 0, 0))))))

    rules.save()
    print(f"✅ V45.3 执行完毕！请检阅完美的涂装与风控表单。")

if __name__ == "__main__":
    main()
