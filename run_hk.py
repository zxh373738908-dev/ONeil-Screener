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
# 🧠 2. V750 增强演算引擎 (时空锁 EOD Lock 版)
# ==========================================
def calculate_advanced_v750(df, hsi_series):
    try:
        df = df.dropna(subset=['Close', 'High', 'Low', 'Volume', 'Open'])
        if len(df) < 252: return None
        
        # --- 【⏳ 时空锁机制 (核心)】 ---
        current_hkt = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
        last_date = df.index[-1].date()
        
        # 如果当前是盘中（未到下午4点），最后一行是今天未收盘的K线 -> 强制剥离！
        is_live_session = (last_date == current_hkt.date() and current_hkt.hour < 16)
        
        if is_live_session:
            live_price = float(df['Close'].iloc[-1]) # 拿今天的实时价格备用
            df_setup = df.iloc[:-1]                  # 形态演算严格回退到“昨天收盘”！
        else:
            live_price = float(df['Close'].iloc[-1])
            df_setup = df                            # 已经收盘，直接用完整数据
            
        if len(df_setup) < 252: return None

        close = df_setup['Close'].values.astype(float)
        open_p = df_setup['Open'].values.astype(float)
        high = df_setup['High'].values.astype(float)
        low = df_setup['Low'].values.astype(float)
        vol = df_setup['Volume'].values.astype(float)
        
        # 这个 setup_price 是昨天的收盘价，也是所有信号触发的基础
        setup_price = close[-1] 

        # 【强力滤网】：过滤条形码死水股 
        if np.max(high[-10:]) <= np.min(low[-10:]) * 1.02:
            return None

        # 1. 均线系统 (基于昨天)
        ma5 = np.mean(close[-5:])
        ma10 = np.mean(close[-10:])
        ma20 = np.mean(close[-20:])
        ma50 = np.mean(close[-50:])
        ma200 = np.mean(close[-200:])
        
        is_stage_2 = (setup_price > ma50 > ma200) and (ma200 > np.mean(close[-220:-200]))

        # 2. RS 相对强度
        hsi_val = hsi_series.reindex(df_setup.index).ffill().values
        rs_line = close / hsi_val
        rs_velocity = (rs_line[-1] - rs_line[-10]) / rs_line[-10] * 100
        rs_nh = rs_line[-1] >= np.max(rs_line[-120:]) 

        # 3. 基础指标计算
        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100
        avg_vol20 = np.mean(vol[-20:])
        
        # 因为我们已经剥离了今天的假数据，这里的 vol[-1] 就是确切的【昨天全天真实成交量】
        vol_surge = vol[-1] / (avg_vol20 + 1) 
        vdu = vol[-1] < avg_vol20 * 0.55 
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100

        # ---- 【🔥 暗影雷达】 ----
        avg_vol20_series = pd.Series(vol).rolling(20).mean().values
        pocket_pivot = False
        # 扫描过去3天 (包含昨天)
        for i in range(-3, 0):
            if vol[i] > avg_vol20_series[i] * 1.2 and close[i] > open_p[i]:
                day_range = high[i] - low[i]
                if day_range > 0 and (close[i] - low[i]) / day_range > 0.5:
                    pocket_pivot = True
                    break

        # ---- 【🚀 真·凌厉进攻】 ----
        is_momentum_attack = (
            (setup_price > ma5) and (ma5 > ma10) and (ma10 > ma20) and (ma20 > ma50) and 
            (setup_price >= np.max(close[-60:]) * 0.95) and
            (rs_velocity > 0) and 
            (adr_20 > 2.5)        
        )

        is_reversal = (setup_price < ma200) and (setup_price > ma20) and pocket_pivot

        # 5. 战法动作分配
        action = "观察"
        prio = 50
        
        if is_momentum_attack:
            action, prio = "⚔️ 凌厉进攻(Momentum)", 96
        elif rs_nh and setup_price < np.max(close[-20:]) * 1.02 and tightness < 1.8:
            action, prio = "👁️ 奇點先行(Stealth)", 95
        elif is_stage_2 and vdu and tightness < 1.5:
            action, prio = "🐉 老龍回頭(V-Dry)", 90
        elif rs_nh and setup_price >= np.max(close[-252:]) and vol_surge > 1.3:
            action, prio = "🚀 巔峰突破(Breakout)", 92
        elif is_reversal:
            action, prio = "🌊 底部巨龙(Reversal)", 85

        # ---- 【☠️ 筹码峰高空引力网】 ----
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

        dist_poc = ((setup_price - poc_price) / poc_price) * 100

        if (action == "观察" or "老龍" in action) and dist_poc > 15:
            action = "☠️ 极度延伸(禁买)"
            prio = 10 
        elif action != "观察" and dist_poc > 45: 
            action = "☠️ 高空危楼(禁买)" 
            prio = 10

        # ---- 【📉 实时盘面校验 (防追高/防破位)】 ----
        today_pct = ((live_price - setup_price) / setup_price) * 100
        
        if action != "观察" and "禁买" not in action:
            if today_pct > 4.5:
                action += " 🚀(已飞勿追)"
                # 分数微降，但不沉底，留作回调观察
                prio -= 5 
            elif today_pct < -3.0:
                action += " 🩸(破位取消)"
                prio = 10 # 跌破昨天形态，直接报废

        # 6. 多重结构止损 (基于昨天的结构)
        adr_stop = setup_price * (1 - adr_20 * 0.01 * 1.6)
        if "进攻" in action or "主升浪" in action:
            struct_stop = ma20 * 0.98 
        elif "底部巨龙" in action:
            struct_stop = low[-3] * 0.95 
        else:
            struct_stop = ma50 * 0.99
            
        final_stop = max(adr_stop, struct_stop) 

        # 7. 真实风控 (根据今天的实时价格来计算你需要买多少股)
        risk_per_share = live_price - final_stop
        suggested_shares = 0
        if risk_per_share > 0:
            suggested_shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // risk_per_share

        return {
            "Action": action, 
            "Base_Score": prio, 
            "Live_Price": round(live_price, 3),   # 实时现价
            "Today_Pct": round(today_pct, 2),     # 今日涨幅监控
            "Tight": round(tightness, 2), 
            "Vol_Ratio": round(vol_surge, 2), 
            "RS_Vel": round(rs_velocity, 2),
            "Dist_POC%": round(dist_poc, 2),
            "PocketPivot": "🔥 发现" if pocket_pivot else "",
            "ADR": round(adr_20, 2), 
            "Stop": round(final_stop, 3),
            "Shares": int(suggested_shares), 
            "rs_raw": (setup_price/close[-63]*2 + setup_price/close[-126] + setup_price/close[-252])
        }
    except Exception as e: 
        return None

# ==========================================
# 🚀 3. 执行流程 
# ==========================================
def main():
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M')
    print(f"[{now_str}] ⏳ 启动时空锁，回拨至昨日收盘抓潜伏...")
    
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

    res_df['Final_Score'] = (
        res_df['Base_Score'] * 1.5 +  
        res_df['rs_raw'].rank(pct=True) * 20 + 
        np.maximum(0, (5 - res_df['Tight']) * 4)  
    ).round(2)

    res_df = res_df.sort_values(by=["Base_Score", "Final_Score"], ascending=[False, False])
    top_picks = res_df.groupby('Sector').head(5) 
    top_picks = top_picks.head(60)

    sh = init_sheet()
    sh.clear()
    
    weather = "☀️ 激进" if hsi_p > hsi_ma50 else "❄️ 观望"
    header = [[f"🏰 V45.2 终极时空锁 (盘前潜伏 + 盘中防追高)", f"环境: {weather}", f"刷新: {now_str}", "风控: 单笔风险 0.8%"]]
    sh.update(range_name="A1", values=header)
    
    # 【新增列】：Today_Pct (今日涨幅%)
    cols =["Ticker", "Action", "Final_Score", "Live_Price", "Today_Pct", "Shares", "Stop", "Tight", "RS_Vel", "PocketPivot", "Dist_POC%", "ADR", "Sector"]
    sh.update(range_name="A3", values=[cols] + top_picks[cols].values.tolist(), value_input_option="USER_ENTERED")

    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:M3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))
    
    rules = get_conditional_format_rules(sh)
    rules.clear() 
    
    # 动态警告标签：已飞勿追 / 破位取消
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['已飞勿追']),
                                format=cellFormat(backgroundColor=color(1.0, 0.7, 0.7), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0, 0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['破位取消']),
                                format=cellFormat(backgroundColor=color(0.3, 0.3, 0.3), textFormat=textFormat(bold=True, strikethrough=True, foregroundColor=color(0.8, 0.8, 0.8))))))
    
    # 正常战法高亮
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['⚔️']),
                                format=cellFormat(backgroundColor=color(1.0, 0.84, 0.0), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0.2, 0.0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🌊']),
                                format=cellFormat(backgroundColor=color(0.8, 0.9, 1.0), textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0.5))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['👁️']),
                                format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🚀']),
                                format=cellFormat(backgroundColor=color(1.0, 0.9, 0.8), textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0.2, 0.2))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['☠️']),
                                format=cellFormat(backgroundColor=color(0.8, 0.8, 0.8), textFormat=textFormat(bold=True, strikethrough=True, foregroundColor=color(0.4, 0.4, 0.4))))))
                                
    # 今日涨幅 (红涨绿跌)
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['0']),
                                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0, 0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_LESS', ['0']),
                                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0.6, 0))))))

    rules.save()
    print(f"✅ 时空锁闭合。您现在看到的是完美的盘前复盘表！")

if __name__ == "__main__":
    main()
