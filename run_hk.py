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
# 🧠 2. V750 增强演算引擎 (肃清伪动能版)
# ==========================================
def calculate_advanced_v750(df, hsi_series):
    try:
        df = df.dropna(subset=['Close', 'High', 'Low', 'Volume', 'Open'])
        if len(df) < 252: return None
        
        close = df['Close'].values.astype(float)
        open_p = df['Open'].values.astype(float)
        high = df['High'].values.astype(float)
        low = df['Low'].values.astype(float)
        vol = df['Volume'].values.astype(float)
        cp = close[-1]
        
        # 【强力滤网】：过滤条形码死水股 
        if np.max(high[-10:]) <= np.min(low[-10:]) * 1.02:
            return None

        # 1. 均线系统
        ma5 = np.mean(close[-5:])
        ma10 = np.mean(close[-10:])
        ma20 = np.mean(close[-20:])
        ma50 = np.mean(close[-50:])
        ma200 = np.mean(close[-200:])
        
        is_stage_2 = (cp > ma50 > ma200) and (ma200 > np.mean(close[-220:-200]))

        # 2. RS 相对强度
        hsi_val = hsi_series.reindex(df.index).ffill().values
        rs_line = close / hsi_val
        rs_velocity = (rs_line[-1] - rs_line[-10]) / rs_line[-10] * 100
        rs_nh = rs_line[-1] >= np.max(rs_line[-120:]) 

        # 3. 基础指标计算
        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100
        avg_vol20 = np.mean(vol[-20:])
        
        # 修正：处理盘中运行时的缩量假象。如果当前是盘中，vol[-1]是没有参考价值的，我们主要看昨日和前日的异动
        vol_surge = vol[-2] / (avg_vol20 + 1) # 改为看昨天是否爆量
        vdu = vol[-2] < avg_vol20 * 0.55 # 昨天是否缩量

        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100

        # ---- 【🔥 补丁1：口袋枢轴 时空修正】 ----
        avg_vol20_series = pd.Series(vol).rolling(20).mean().values
        pocket_pivot = False
        # 从 -4 到 -1，只扫描已经收盘的确定的K线（防早盘半截子成交量干扰）
        for i in range(-4, -1):
            if vol[i] > avg_vol20_series[i] * 1.2 and close[i] > open_p[i]:
                day_range = high[i] - low[i]
                if day_range > 0 and (close[i] - low[i]) / day_range > 0.5:
                    pocket_pivot = True
                    break

        # ---- 【🚀 补丁2：真·凌厉进攻 (排除银行/公用事业)】 ----
        is_momentum_attack = (
            (cp > ma5) and (ma5 > ma10) and (ma10 > ma20) and (ma20 > ma50) and 
            (cp >= np.max(close[-60:]) * 0.95) and
            (rs_velocity > 0) and # 核心：必须跑赢恒指
            (adr_20 > 2.5)        # 核心：近期要有振幅和弹性，不要死水
        )

        is_reversal = (cp < ma200) and (cp > ma20) and pocket_pivot

        # 5. 战法动作分配
        action = "观察"
        prio = 50
        
        if is_momentum_attack:
            action, prio = "⚔️ 凌厉进攻(Momentum)", 96
        elif rs_nh and cp < np.max(close[-20:]) * 1.02 and tightness < 1.8:
            action, prio = "👁️ 奇點先行(Stealth)", 95
        elif is_stage_2 and vdu and tightness < 1.5:
            action, prio = "🐉 老龍回頭(V-Dry)", 90
        elif rs_nh and cp >= np.max(close[-252:]) and vol_surge > 1.3:
            action, prio = "🚀 巔峰突破(Breakout)", 92
        elif is_reversal:
            action, prio = "🌊 底部巨龙(Reversal)", 85

        # ---- 【☠️ 补丁3：重设高空引力网 (45%生死线)】 ----
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

        # 核心修正：
        # 1. 杂毛股/老龙回头，超15%就不买。
        # 2. 哪怕你是 ⚔️凌厉进攻，只要离主力成本超 45% (说明进入纯博傻阶段)，强制熔断变☠️禁买！
        if (action == "观察" or "老龍" in action) and dist_poc > 15:
            action = "☠️ 极度延伸(禁买)"
            prio = 10 
        elif action != "观察" and dist_poc > 45: 
            action = "☠️ 高空危楼(禁买)" # 专门区分因涨太多被毙掉的妖股
            prio = 10

        # 6. 多重结构止损
        adr_stop = cp * (1 - adr_20 * 0.01 * 1.6)
        if "进攻" in action or "主升浪" in action:
            struct_stop = ma20 * 0.98 
        elif "底部巨龙" in action:
            struct_stop = low[-3] * 0.95 
        else:
            struct_stop = ma50 * 0.99
            
        final_stop = max(adr_stop, struct_stop) 

        risk_per_share = cp - final_stop
        suggested_shares = 0
        if risk_per_share > 0:
            suggested_shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // risk_per_share

        return {
            "Action": action, 
            "Base_Score": prio, 
            "Price": round(cp, 3),
            "Tight": round(tightness, 2), 
            "Vol_Ratio": round(vol_surge, 2), # 这里显示的是昨天的爆发比，盘中不失真
            "RS_Vel": round(rs_velocity, 2),
            "Dist_POC%": round(dist_poc, 2),
            "PocketPivot": "🔥 发现" if pocket_pivot else "",
            "ADR": round(adr_20, 2), 
            "Stop": round(final_stop, 3),
            "Shares": int(suggested_shares), 
            "rs_raw": (cp/close[-63]*2 + cp/close[-126] + cp/close[-252])
        }
    except Exception as e: 
        return None

# ==========================================
# 🚀 3. 执行流程 
# ==========================================
def main():
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🚀 肃清版启动，清洗伪动能...")
    
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

    final_list = []
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
    header = [[f"🏰 V45.1 终极净化版 (排除伪动能+高空熔断)", f"环境: {weather}", f"刷新: {now_str}", "风控: 单笔风险 0.8%"]]
    sh.update(range_name="A1", values=header)
    
    # 调整列顺序，把重要的放前面
    cols =["Ticker", "Action", "Final_Score", "Price", "Shares", "Stop", "Tight", "RS_Vel", "PocketPivot", "Dist_POC%", "Vol_Ratio", "ADR", "Sector"]
    sh.update(range_name="A3", values=[cols] + top_picks[cols].values.tolist(), value_input_option="USER_ENTERED")

    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:M3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))
    
    rules = get_conditional_format_rules(sh)
    rules.clear() 
    
    # ⚔️ 凌厉进攻
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['⚔️']),
                                format=cellFormat(backgroundColor=color(1.0, 0.84, 0.0), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0.2, 0.0))))))
                                
    # 🌊 底部巨龙
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🌊']),
                                format=cellFormat(backgroundColor=color(0.8, 0.9, 1.0), textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0.5))))))
    
    # 👁️ 奇点先行
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['👁️']),
                                format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
                                
    # 🚀 主升浪
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🚀']),
                                format=cellFormat(backgroundColor=color(1.0, 0.9, 0.8), textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0.2, 0.2))))))
                                
    # ☠️ 极度延伸 / 高空危楼 (深灰色+删除线，警示不可买入)
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['☠️']),
                                format=cellFormat(backgroundColor=color(0.8, 0.8, 0.8), textFormat=textFormat(bold=True, strikethrough=True, foregroundColor=color(0.4, 0.4, 0.4))))))
                                
    # 建议股数
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['0']),
                                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0.5, 0))))))
                                
    # 🔥 Pocket Pivot
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('I4:I100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🔥']),
                                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.9, 0.1, 0.1))))))
                                
    # Dist_POC% 绿色安全区 (-10 到 20)
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('J4:J100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_BETWEEN',['-10', '20']),
                                format=cellFormat(textFormat=textFormat(foregroundColor=color(0, 0.6, 0))))))
                                
    # Dist_POC% 红色危险区 (> 45)
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('J4:J100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER',['45']),
                                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.9, 0, 0))))))

    rules.save()
    print(f"✅ 净化完毕。大笨象已驱逐，高危股已被封杀。")

if __name__ == "__main__":
    main()
