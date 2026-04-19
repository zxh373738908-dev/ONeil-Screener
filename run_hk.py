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
# 🧠 2. V750 增强演算引擎 (释放 VCP)
# ==========================================
def calculate_advanced_v750(df, hsi_series):
    try:
        df = df.dropna(subset=['Close', 'High', 'Low', 'Volume', 'Open'])
        if len(df) < 252: return None
        
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

        action, prio = "观察", 50
        
        # 🟢 V45.4 规则触发
        if rs_nh and setup_price < np.max(close[-20:]) * 1.04 and tightness < 2.5:
            action, prio = "👁️ 奇點先行(Stealth)", 98 
        elif rs_nh and setup_price >= np.max(close[-252:]) and vol_surge > 1.3:
            action, prio = "🚀 巔峰突破(Breakout)", 95 
        elif is_momentum_attack:
            action, prio = "⚔️ 凌厉进攻(Momentum)", 92 
        elif is_stage_2 and vdu and tightness < 2.0:
            action, prio = "🐉 老龍回頭(V-Dry)", 90
        elif is_reversal:
            action, prio = "🌊 底部巨龙(Reversal)", 88

        # 延伸度校验
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

        today_pct = ((live_price - setup_price) / setup_price) * 100
        if action != "观察" and "禁买" not in action:
            if today_pct > 4.5:
                action += " 🚀(已飞勿追)"
                prio -= 5 
            elif today_pct < -3.0:
                action += " 🩸(破位取消)"
                prio = 10 

        # ========== 高级扩展指标计算 ==========
        bias20 = ((live_price - ma20) / ma20) * 100
        
        # 绝对收益率 REL5/20/60/120
        ret_5d = ((live_price - close[-5]) / close[-5]) * 100
        ret_20d = ((live_price - close[-20]) / close[-20]) * 100
        ret_60d = ((live_price - close[-60]) / close[-60]) * 100
        ret_120d = ((live_price - close[-120]) / close[-120]) * 100
        
        # 相对恒指强度 R20/60/120
        r20 = ((rs_line[-1] - rs_line[-20]) / rs_line[-20]) * 100
        r60 = ((rs_line[-1] - rs_line[-60]) / rs_line[-60]) * 100
        r120 = ((rs_line[-1] - rs_line[-120]) / rs_line[-120]) * 100
        
        # 60日趋势判定
        ma60 = np.mean(close[-60:])
        trend_60d = f"↗ {(live_price - ma60)/ma60*100:.1f}%" if live_price >= ma60 else f"↘ {(live_price - ma60)/ma60*100:.1f}%"

        # 2024-12-31 至今涨幅
        df_2024 = df_setup[df_setup.index <= '2024-12-31']
        if not df_2024.empty:
            price_2024_end = float(df_2024['Close'].iloc[-1])
        else:
            price_2024_end = float(close[-252]) if len(close) >= 252 else float(close[0])
        ret_from_2024 = ((live_price - price_2024_end) / price_2024_end) * 100

        # 共振雷达
        if pocket_pivot and rs_nh: resonance = "🔥 量价共振"
        elif pocket_pivot: resonance = "🔥 口袋支点"
        elif rs_nh: resonance = "✨ 相对强势"
        else: resonance = "-"
        
        return {
            "Action": action, "Base_Score": prio, 
            "Price": round(live_price, 3), 
            "Tight": round(tightness, 2), "Vol_Ratio": round(vol_surge, 2), 
            "Resonance": resonance, "ADR": round(adr_20, 2),
            "Bias": round(bias20, 2), 
            "1D%": round(today_pct, 2), "60D Trend": trend_60d,
            "REL5": round(ret_5d, 2), "REL20": round(ret_20d, 2), 
            "REL60": round(ret_60d, 2), "REL120": round(ret_120d, 2),
            "R20": round(r20, 2), "R60": round(r60, 2), "R120": round(r120, 2),
            "From 2024-12-31": round(ret_from_2024, 2),
            "rs_raw": (setup_price/close[-63]*2 + setup_price/close[-126] + setup_price/close[-252])
        }
    except Exception as e: return None

def main():
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🦅 V45.4 矩阵增强版启动...")
    
    hsi_raw = yf.download("^HSI", period="300d", progress=False)['Close']
    hsi_series = hsi_raw.iloc[:,0] if isinstance(hsi_raw, pd.DataFrame) else hsi_raw
    hsi_p, hsi_ma50 = hsi_series.iloc[-1], hsi_series.rolling(50).mean().iloc[-1]
    
    url = "https://scanner.tradingview.com/hongkong/scan"
    # 使用 industry 代替 sector 获取更精准的行业
    payload = {"columns":["name", "description", "close", "market_cap_basic", "industry"],
               "filter":[{"left": "market_cap_basic", "operation": "greater", "right": 1.2e10}],
               "range":[0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        resp = requests.post(url, json=payload, timeout=15).json().get('data',[])
        df_pool = pd.DataFrame([{
            "code": re.sub(r'[^0-9]', '', d['d'][0]), 
            "industry": d['d'][4] or "其他",
            "mkt_cap": d['d'][3] or 0
        } for d in resp])
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
                row_data = df_pool[df_pool['code']==code_raw].iloc[0]
                mkt_cap_fmt = f"{round(row_data['mkt_cap'] / 1e8, 1)}亿" 
                res.update({"Ticker": t.split('.')[0], "Industry": row_data['industry'], "MktCap": mkt_cap_fmt})
                final_list.append(res)
        except: continue

    if not final_list: return
    res_df = pd.DataFrame(final_list)

    # 计算最终得分与全局 RS 排名
    res_df['Score'] = (res_df['Base_Score'] * 1.5 + res_df['rs_raw'].rank(pct=True) * 20 + np.maximum(0, (5 - res_df['Tight']) * 4)).round(2)
    res_df['Rank'] = (res_df['rs_raw'].rank(pct=True) * 100).round(1)
    
    res_df = res_df.sort_values(by=["Base_Score", "Score"], ascending=[False, False])
    top_picks = res_df.groupby('Industry').head(5).head(60)

    sh = init_sheet()
    sh.clear()
    
    weather = "☀️ 激进" if hsi_p > hsi_ma50 else "❄️ 观望"
    header = [[f"🏰 V45.4 全列矩阵版", f"环境: {weather}", f"刷新: {now_str}", "风控: 单笔风险 0.8%"]]
    sh.update(range_name="A1", values=header)
    
    # 🎯 全新指定的 21 个核心展示列
    cols =[
        "Ticker", "Industry", "Score", "1D%", "60D Trend", "Action", "Resonance", 
        "ADR", "Vol_Ratio", "Bias", "MktCap", "Rank", 
        "REL5", "REL20", "REL60", "REL120", "R20", "R60", "R120", 
        "Price", "From 2024-12-31"
    ]
    
    sh.update(range_name="A3", values=[cols] + top_picks[cols].values.tolist(), value_input_option="USER_ENTERED")

    # 冻结表头与格式化
    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:U3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))
    
    rules = get_conditional_format_rules(sh)
    rules.clear() 
    
    # [F列] Action 动态警告标签
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('F4:F100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['已飞勿追']), format=cellFormat(backgroundColor=color(1.0, 0.7, 0.7), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0, 0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('F4:F100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['破位取消']), format=cellFormat(backgroundColor=color(0.3, 0.3, 0.3), textFormat=textFormat(bold=True, strikethrough=True, foregroundColor=color(0.8, 0.8, 0.8))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('F4:F100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['🚀']), format=cellFormat(backgroundColor=color(1.0, 0.6, 0.2), textFormat=textFormat(bold=True, foregroundColor=color(0.4, 0.1, 0.0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('F4:F100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['👁️']), format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('F4:F100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['⚔️']), format=cellFormat(backgroundColor=color(1.0, 0.84, 0.0), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0.2, 0.0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('F4:F100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🌊']), format=cellFormat(backgroundColor=color(0.8, 0.9, 1.0), textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0.5))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('F4:F100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['☠️']), format=cellFormat(backgroundColor=color(0.85, 0.85, 0.85), textFormat=textFormat(bold=True, strikethrough=True, foregroundColor=color(0.5, 0.5, 0.5))))))
    
    # [G列] Resonance 共振标签
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('G4:G100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS',['🔥']), format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.9, 0.1, 0.1))))))

    # [E列] 60D Trend 趋势文字颜色
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['↗']), format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0, 0))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)], booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['↘']), format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0.6, 0))))))

    # 数值类红绿配色对齐 D(1D%), J(Bias), M:S(REL/R指标), U(From 2024-12-31)
    ranges_to_color =[
        GridRange.from_a1_range('D4:D100', sh), 
        GridRange.from_a1_range('J4:J100', sh), 
        GridRange.from_a1_range('M4:S100', sh), 
        GridRange.from_a1_range('U4:U100', sh)
    ]
    rules.append(ConditionalFormatRule(ranges=ranges_to_color, booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['0']), format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0, 0))))))
    rules.append(ConditionalFormatRule(ranges=ranges_to_color, booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_LESS', ['0']), format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0.6, 0))))))
    
    rules.save()
    print(f"✅ V45.4 矩阵列同步完毕！")

if __name__ == "__main__":
    main()
