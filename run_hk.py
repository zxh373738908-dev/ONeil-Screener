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
# 🧠 2. V750 增强演算引擎 (修复霸榜bug + 捕获逼空进攻)
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
        
        # 【强力滤网】：过滤条形码死水股 (近10天最高价和最低价几乎没区别)
        if np.max(high[-10:]) <= np.min(low[-10:]) * 1.02:
            return None

        # 1. 均线系统
        ma5 = np.mean(close[-5:])
        ma10 = np.mean(close[-10:])
        ma20 = np.mean(close[-20:])
        ma50 = np.mean(close[-50:])
        ma200 = np.mean(close[-200:])
        
        # 基础趋势判定
        is_stage_2 = (cp > ma50 > ma200) and (ma200 > np.mean(close[-220:-200]))

        # 【🚀 进攻雷达】：捕获 00522 / 01888 这种完美的均线多头逼空排列
        is_momentum_attack = (
            (cp > ma5) and (ma5 > ma10) and (ma10 > ma20) and (ma20 > ma50) and 
            (cp >= np.max(close[-60:]) * 0.95) # 距离近2个月高点不到5%，强势逼空
        )

        is_main_uptrend = (
            (cp > ma10) and (cp > ma20) and 
            (ma10 > ma50) and (ma20 > ma50) and 
            (cp >= np.max(close[-60:]) * 0.85)
        )

        # 2. RS 相对强度
        hsi_val = hsi_series.reindex(df.index).ffill().values
        rs_line = close / hsi_val
        rs_velocity = (rs_line[-1] - rs_line[-10]) / rs_line[-10] * 100
        rs_nh = rs_line[-1] >= np.max(rs_line[-120:]) # 放宽到半年新强，不要拘泥于一年

        # 3. VCP 紧致度 
        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100
        
        # 4. 机构能量 
        avg_vol20 = np.mean(vol[-20:])
        vol_surge = vol[-1] / (avg_vol20 + 1)
        vdu = vol[-1] < avg_vol20 * 0.55 

        # ---- 【🔥 补丁1：高阶口袋枢轴 (真金白银买盘)】 ----
        avg_vol20_series = pd.Series(vol).rolling(20).mean().values
        pocket_pivot = False
        for i in range(-3, 0):
            # 放量 + 收阳 + 收盘价在全天振幅上半区(拒绝长上影线骗炮)
            if vol[i] > avg_vol20_series[i] * 1.2 and close[i] > open_p[i]:
                day_range = high[i] - low[i]
                if day_range > 0 and (close[i] - low[i]) / day_range > 0.5:
                    pocket_pivot = True
                    break
                
        # ---- 【🌊 补丁2：抄底起爆】 ----
        is_reversal = (cp < ma200) and (cp > ma20) and pocket_pivot

        # 5. 战法动作分配
        action = "观察"
        prio = 50
        
        # 最高优先级：凌厉进攻 (匹配你的00522图形)
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

        # 兜底补充主升浪
        if action == "观察" and is_main_uptrend:
            action, prio = "🚀 主升浪(Uptrend)", 80

        # ---- 【☠️ 补丁3：筹码峰 (POC) 过滤修正】 ----
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

        # 修正：绝不能错杀进攻中的票！只过滤垃圾股和老龙回头的延伸
        if (action == "观察" or "老龍" in action) and dist_poc > 15:
            action = "☠️ 极度延伸(禁买)"
            prio = 10 

        # 6. 多重结构止损
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
        adr_stop = cp * (1 - adr_20 * 0.01 * 1.6)
        
        if "进攻" in action or "主升浪" in action:
            struct_stop = ma20 * 0.98 # 进攻态势，跌破MA20直接走人，不扛单
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
            "Vol_Ratio": round(vol_surge, 2), 
            "ADR": round(adr_20, 2), 
            "Stop": round(final_stop, 3),
            "Shares": int(suggested_shares), 
            "RS_Vel": round(rs_velocity, 2),
            "Dist_POC%": round(dist_poc, 2),
            "PocketPivot": "🔥 发现" if pocket_pivot else "",
            "rs_raw": (cp/close[-63]*2 + cp/close[-126] + cp/close[-252])
        }
    except Exception as e: 
        return None

# ==========================================
# 🚀 3. 执行流程 (安全评分体系)
# ==========================================
def main():
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🚀 终极猎杀版启动...")
    
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

    # 【终极修正】：摒弃让死水股霸榜的指数算法。采用温和的线性降权，并优先看 Base_Score (战法等级)。
    # Tight 如果在合理的 1~4 之间，会获得正常加分。大于 5 不加分。
    res_df['Final_Score'] = (
        res_df['Base_Score'] * 1.5 +  # 绝对尊重战法图形本身的优先级
        res_df['rs_raw'].rank(pct=True) * 20 + 
        np.maximum(0, (5 - res_df['Tight']) * 4)  # 线性温和奖励紧致度，防止被老千股撑爆
    ).round(2)

    # 排序时，确保 ☠️极度延伸 等级永远垫底，优先把 ⚔️ 🚀 选出来
    res_df = res_df.sort_values(by=["Base_Score", "Final_Score"], ascending=[False, False])
    top_picks = res_df.groupby('Sector').head(5) # 放宽到每个板块前5名
    top_picks = top_picks.head(60)

    # 5. 写入与可视化
    sh = init_sheet()
    sh.clear()
    
    weather = "☀️ 激进" if hsi_p > hsi_ma50 else "❄️ 观望"
    header = [[f"🏰 V45-全天候猎杀版 (含⚔️凌厉进攻)", f"环境: {weather}", f"刷新: {now_str}", "风控: 单笔风险 0.8%"]]
    sh.update(range_name="A1", values=header)
    
    cols =["Ticker", "Action", "Final_Score", "Price", "Shares", "Stop", "Tight", "Vol_Ratio", "RS_Vel", "Dist_POC%", "PocketPivot", "ADR", "Sector"]
    sh.update(range_name="A3", values=[cols] + top_picks[cols].values.tolist(), value_input_option="USER_ENTERED")

    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:M3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))
    
    rules = get_conditional_format_rules(sh)
    rules.clear() 
    
    # 新增 ⚔️ 凌厉进攻 - 亮眼金黄色（最强动能）
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['⚔️']),
                                format=cellFormat(backgroundColor=color(1.0, 0.84, 0.0), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0.2, 0.0))))))
                                
    # 🌊 底部巨龙
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🌊']),
                                format=cellFormat(backgroundColor=color(0.8, 0.9, 1.0), textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0.5))))))
    
    # 奇点先行
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['👁️']),
                                format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
                                
    # 主升浪 / 巅峰突破
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🚀']),
                                format=cellFormat(backgroundColor=color(1.0, 0.9, 0.8), textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0.2, 0.2))))))
                                
    # ☠️ 极度延伸
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['☠️']),
                                format=cellFormat(backgroundColor=color(0.85, 0.85, 0.85), textFormat=textFormat(bold=True, strikethrough=True, foregroundColor=color(0.5, 0.5, 0.5))))))
                                
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['0']),
                                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0.5, 0))))))
                                
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('K4:K100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🔥']),
                                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.9, 0.1, 0.1))))))
                                
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('J4:J100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_BETWEEN',['-10', '15']),
                                format=cellFormat(textFormat=textFormat(foregroundColor=color(0, 0.6, 0))))))

    rules.save()
    print(f"✅ 任务完成。死水股已被清理，已释放凌厉进攻型席位。")

if __name__ == "__main__":
    main()
