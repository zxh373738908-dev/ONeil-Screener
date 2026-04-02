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
# 🧠 2. V750 增强演算引擎 (🚀主升浪 补丁版)
# ==========================================
def calculate_advanced_v750(df, hsi_series):
    try:
        df = df.dropna(subset=['Close'])
        if len(df) < 252: return None
        
        close = df['Close'].values.astype(float)
        high = df['High'].values.astype(float)
        low = df['Low'].values.astype(float)
        vol = df['Volume'].values.astype(float)
        cp = close[-1]
        
        # 1. 趋势模板与生命线
        ma10 = np.mean(close[-10:])
        ma20 = np.mean(close[-20:])
        ma50 = np.mean(close[-50:])
        ma200 = np.mean(close[-200:])
        is_stage_2 = (cp > ma50 > ma200) and (ma200 > np.mean(close[-220:-200]))

        # ---- 【🚀 补丁1：主升浪特征提取】 ----
        # 解决 00175/02315 等强势股错杀：允许短期MA因波动粘合，但股价已强劲站上MA10与MA20，且不背离近期高点
        is_main_uptrend = (
            (cp > ma10) and (cp > ma20) and 
            (ma10 > ma50) and (ma20 > ma50) and 
            (ma20 > np.mean(close[-25:-5])) and 
            (cp >= np.max(close[-60:]) * 0.85)
        )

        # 2. RS 加速度 (IBD 模拟)
        hsi_val = hsi_series.reindex(df.index).ffill().values
        rs_line = close / hsi_val
        # RS 线不仅看新高，看斜率 (近10日涨幅)
        rs_velocity = (rs_line[-1] - rs_line[-10]) / rs_line[-10] * 100
        rs_nh = rs_line[-1] >= np.max(rs_line[-252:])

        # 3. VCP 紧致度 (极致收缩判断)
        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100
        
        # 4. 机构能量 (成交额爆发比)
        avg_vol20 = np.mean(vol[-20:])
        vol_surge = vol[-1] / avg_vol20
        vdu = vol[-1] < avg_vol20 * 0.55 # 成交量枯竭

        # 5. 综合战法判定
        action = "观察"
        prio = 50
        if rs_nh and cp < np.max(close[-20:]) * 1.02 and tightness < 1.4:
            action, prio = "👁️ 奇點先行(Stealth)", 95
        elif is_stage_2 and vdu and tightness < 1.2:
            action, prio = "🐉 老龍回頭(V-Dry)", 90
        elif rs_nh and cp >= np.max(close[-252:]) and vol_surge > 1.3:
            action, prio = "🚀 巔峰突破(Breakout)", 92
        elif is_stage_2 and rs_nh and rs_velocity > 0:
            action, prio = "💎 雙重共振(Leader)", 88

        # ---- 【🚀 补丁2：主升浪判定与超级加分】 ----
        if is_main_uptrend:
            if action == "观察":
                action, prio = "🚀 主升浪(Uptrend)", 94
            else:
                # 叠加了主升浪光环的个股，彻底拉爆优先级（+10分保送）
                action = action.replace(")", " + 🚀主升浪)")
                prio += 10

        # 6. 多重结构止损 (取 MA50 与 ADR 止损的科学平衡)
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
        adr_stop = cp * (1 - adr_20 * 0.01 * 1.6)
        
        # ---- 【🚀 补丁3：主升浪动态防守】 ----
        if "主升浪" in action:
            struct_stop = ma20 * 0.98 # 主升浪跌破 MA20的2% 及时离场，避免利润大幅回撤
        else:
            struct_stop = ma50 * 0.99
            
        final_stop = max(adr_stop, struct_stop) # 哪个近用哪个，保护利润

        # 7. 建议仓位 (Risk Parity 模型)
        risk_per_share = cp - final_stop
        suggested_shares = 0
        if risk_per_share > 0:
            suggested_shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // risk_per_share

        return {
            "Action": action, "Score": prio + (rs_velocity * 2), "Price": cp, 
            "Tight": round(tightness, 2), "Vol_Ratio": round(vol_surge, 2), 
            "ADR": round(adr_20, 2), "Stop": round(final_stop, 2),
            "Shares": int(suggested_shares), "RS_Vel": round(rs_velocity, 2),
            "is_bull": cp > ma200, "rs_raw": (cp/close[-63]*2 + cp/close[-126] + cp/close[-252])
        }
    except: return None

# ==========================================
# 🚀 3. 执行流程 (包含板块配额与量子评分)
# ==========================================
def main():
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V45-V750 Pro Max 启动...")
    
    # 1. 抓取基准
    hsi_raw = yf.download("^HSI", period="300d", progress=False)['Close']
    hsi_series = hsi_raw.iloc[:,0] if isinstance(hsi_raw, pd.DataFrame) else hsi_raw
    hsi_p, hsi_ma50 = hsi_series.iloc[-1], hsi_series.rolling(50).mean().iloc[-1]
    
    # 2. 扫描 TradingView 票池
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "description", "close", "market_cap_basic", "sector"],
               "filter":[{"left": "market_cap_basic", "operation": "greater", "right": 1.2e10}],
               "range": [0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        resp = requests.post(url, json=payload, timeout=15).json().get('data',[])
        df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "其他"} for d in resp])
    except: return

    # 3. 获取个股详情
    final_list = []
    tickers =[str(c).zfill(4)+".HK" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    for t in tickers:
        try:
            code_raw = t.split('.')[0].lstrip('0')
            if t not in data.columns.levels[0]: continue
            res = calculate_advanced_v750(data[t], hsi_series)
            if res and res['is_bull'] and res['Action'] != "观察":
                res.update({"Ticker": t.split('.')[0], "Sector": df_pool[df_pool['code']==code_raw].iloc[0]['sector']})
                final_list.append(res)
        except: continue

    if not final_list: return
    res_df = pd.DataFrame(final_list)

    # 4. 板块配额与排名：每个板块只展示前 4 强，防止风险集中
    res_df['Final_Score'] = res_df['Score'] + res_df['rs_raw'].rank(pct=True)*20
    top_picks = res_df.sort_values(by="Final_Score", ascending=False).groupby('Sector').head(4)
    top_picks = top_picks.head(60) # 总榜前60

    # 5. 写入与可视化
    sh = init_sheet()
    sh.clear()
    
    weather = "☀️ 激进" if hsi_p > hsi_ma50 else "❄️ 观望"
    header = [[f"🏰 V45-V750 量子领袖版 (🚀主升浪防错杀版)", f"环境: {weather}", f"刷新: {now_str}", "风控: 单笔风险 0.8% / 板块配额制"]]
    sh.update(range_name="A1", values=header)
    
    cols =["Ticker", "Action", "Final_Score", "Price", "Shares", "Stop", "Tight", "Vol_Ratio", "RS_Vel", "ADR", "Sector"]
    sh.update(range_name="A3", values=[cols] + top_picks[cols].values.tolist(), value_input_option="USER_ENTERED")

    # 美化格式
    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:K3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))
    
    rules = get_conditional_format_rules(sh)
    # 奇点先行 - 紫色高亮
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['👁️']),
                                format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
    # 主升浪 / 巅峰突破 - 橙红色高亮 (新增强调)
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🚀']),
                                format=cellFormat(backgroundColor=color(1.0, 0.9, 0.8), textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0.2, 0.2))))))
    # 建议股数 - 绿色提醒
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['0']),
                                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0.5, 0))))))
    rules.save()
    print(f"✅ 任务完成。成功捕捉 {len(top_picks)} 只量子领袖股。")

if __name__ == "__main__":
    main()
