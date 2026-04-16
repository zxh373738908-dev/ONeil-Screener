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
# 建议在 Google Sheet 中手动新建两个工作表，名字分别为 "V750_防御" 和 "V850_进攻"
TAB_NAME_V750 = "V750_防御"
TAB_NAME_V850 = "V850_进攻"
CREDS_FILE = "credentials.json"
ACCOUNT_SIZE = 500000 
MAX_RISK_PER_TRADE = 0.008 

def get_worksheet(doc, title):
    try:
        return doc.worksheet(title)
    except:
        return doc.add_worksheet(title=title, rows="100", cols="20")

def init_sheets():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    return get_worksheet(doc, TAB_NAME_V750), get_worksheet(doc, TAB_NAME_V850)

# ==========================================
# 🧠 2. 核心演算引擎 (V750 & V850)
# ==========================================
def process_engine(df, hsi_series, mode="V750"):
    try:
        df = df.dropna(subset=['Close'])
        if len(df) < 252: return None
        
        close = df['Close'].values.astype(float)
        open_p = df['Open'].values.astype(float)
        high = df['High'].values.astype(float)
        low = df['Low'].values.astype(float)
        vol = df['Volume'].values.astype(float)
        cp = close[-1]
        
        # 基础指标
        ma10, ma20, ma50, ma200 = np.mean(close[-10:]), np.mean(close[-20:]), np.mean(close[-50:]), np.mean(close[-200:])
        avg_vol20 = np.mean(vol[-20:])
        vol_surge = vol[-1] / avg_vol20
        turnover = cp * vol[-1] # 当日成交额 (近似值)

        # RS 与 紧致度
        hsi_val = hsi_series.reindex(df.index).ffill().values
        rs_line = close / hsi_val
        rs_velocity = (rs_line[-1] - rs_line[-10]) / rs_line[-10] * 100
        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100

        # 战法判定逻辑
        is_stage_2 = (cp > ma50 > ma200)
        rs_nh = rs_line[-1] >= np.max(rs_line[-252:])
        
        action = "观察"
        prio = 50
        
        # 1. 战法识别
        if rs_nh and cp < np.max(close[-20:]) * 1.02 and tightness < 1.4:
            action, prio = "👁️ 奇點先行", 95
        elif is_stage_2 and rs_nh and rs_velocity > 0:
            action, prio = "💎 雙重共振", 88
        elif rs_nh and cp >= np.max(close[-252:]) and vol_surge > 1.3:
            action, prio = "🚀 巔峰突破", 92
        elif (cp > ma10 > ma20 > ma50):
            action, prio = "🔥 主升浪", 85

        # ---- 【模式差异化处理】 ----
        if mode == "V850":
            # 进攻版逻辑：取消筹码否决，强制成交额过滤
            if turnover < 4.5e8 or turnover > 5.5e9: # 约5亿-55亿区间
                return None
            
            # 狂暴评分：RS 动量占 60%，成交量占 20%
            final_score = prio + (rs_velocity * 2.5) + (vol_surge * 5) + (1.0 / tightness)
            dist_poc = ((cp - np.mean(close[-126:])) / np.mean(close[-126:])) * 100 # 仅参考
        else:
            # 防御版逻辑：包含 POC 禁令
            hist_close = close[-126:]
            hist_vol = vol[-126:]
            bins = np.linspace(np.min(hist_close), np.max(hist_close), 50)
            vol_bins = np.zeros(50)
            np.add.at(vol_bins, np.clip(np.digitize(hist_close, bins)-1, 0, 49), hist_vol)
            poc_price = bins[np.argmax(vol_bins)]
            dist_poc = ((cp - poc_price) / poc_price) * 100
            
            if dist_poc > 8: 
                action = "☠️ 极度延伸"
                prio = 10
            
            final_score = prio + (rs_velocity * 0.6) + (10 / np.maximum(tightness, 0.1))**2

        # 止损与仓位
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
        final_stop = max(cp * (1 - adr_20 * 0.01 * 1.6), ma20 * 0.98)
        suggested_shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // (cp - final_stop) if cp > final_stop else 0

        return {
            "Ticker": "", "Action": action, "Final_Score": round(final_score, 2),
            "Price": cp, "Shares": int(suggested_shares), "Stop": round(final_stop, 2),
            "Tight": round(tightness, 2), "Vol_Ratio": round(vol_surge, 2), 
            "RS_Vel": round(rs_velocity, 2), "Dist_POC%": round(dist_poc, 2),
            "Turnover(M)": round(turnover/1e6, 0), "Sector": "", "is_bull": cp > ma200
        }
    except: return None

# ==========================================
# 🚀 3. 主程序
# ==========================================
def main():
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V850 狂暴先锋 & V750 联袂启动...")
    
    # 1. 环境分析
    hsi_raw = yf.download("^HSI", period="300d", progress=False)['Close']
    hsi_series = hsi_raw.iloc[:,0] if isinstance(hsi_raw, pd.DataFrame) else hsi_raw
    hsi_cp = hsi_series.iloc[-1]
    hsi_ma20 = hsi_series.rolling(20).mean().iloc[-1]
    hsi_ma50 = hsi_series.rolling(50).mean().iloc[-1]
    
    # 进攻开关
    market_mode = "⚔️ 进攻 (V850 激活)" if hsi_cp > hsi_ma20 else "🛡️ 防御 (谨慎出击)"
    
    # 2. 扫描数据源
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns":["name", "sector"], "filter":[{"left": "market_cap_basic", "operation": "greater", "right": 1e10}], "range":[0, 450]}
    resp = requests.post(url, json=payload).json().get('data',[])
    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][1]} for d in resp])
    
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code']]
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)

    # 3. 运行双引擎
    v750_results, v850_results = [], []
    
    for t in tickers:
        try:
            ticker_data = data[t]
            code_raw = t.split('.')[0].lstrip('0')
            sector = df_pool[df_pool['code']==code_raw].iloc[0]['sector'] or "其他"
            
            # 运行 V750
            res_v750 = process_engine(ticker_data, hsi_series, mode="V750")
            if res_v750 and res_v750['is_bull'] and res_v750['Action'] != "观察":
                res_v750.update({"Ticker": t.split('.')[0], "Sector": sector})
                v750_results.append(res_v750)
                
            # 运行 V850
            res_v850 = process_engine(ticker_data, hsi_series, mode="V850")
            if res_v850 and res_v850['Action'] != "观察":
                res_v850.update({"Ticker": t.split('.')[0], "Sector": sector})
                v850_results.append(res_v850)
        except: continue

    # 4. 写入 Google Sheets
    ws_v750, ws_v850 = init_sheets()
    
    output_configs = [
        {"ws": ws_v750, "data": v750_results, "title": "V750 防御版 (稳健/紧致度优先)"},
        {"ws": ws_v850, "data": v850_results, "title": "V850 狂暴先锋 (进攻/动量优先)"}
    ]

    cols = ["Ticker", "Action", "Final_Score", "Price", "Shares", "Stop", "Tight", "Vol_Ratio", "RS_Vel", "Dist_POC%", "Turnover(M)", "Sector"]

    for config in output_configs:
        ws = config["ws"]
        ws.clear()
        if not config["data"]: continue
        
        df_final = pd.DataFrame(config["data"]).sort_values(by="Final_Score", ascending=False).head(50)
        
        # 表头信息
        header = [[f"🏰 {config['title']}", f"大盘状态: {market_mode}", f"刷新: {now_str}", "风控: 单笔0.8%"]]
        ws.update(range_name="A1", values=header)
        ws.update(range_name="A3", values=[cols] + df_final[cols].values.tolist(), value_input_option="USER_ENTERED")
        
        # 基础格式化
        set_frozen(ws, rows=3)
        format_cell_range(ws, 'A3:L3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0.2, 0.2, 0.2)))
        
        # 条件格式 (基础格式化)
        set_frozen(ws, rows=3)
        format_cell_range(ws, 'A3:L3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0.2, 0.2, 0.2)))
        
        rules = get_conditional_format_rules(ws)
        
        # 1. 针对 V850 的 RS 极速动量 - 金色提醒 (数值类型)
        if "V850" in config["title"]:
            rules.append(ConditionalFormatRule(
                ranges=[GridRange.from_a1_range('I4:I100', ws)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition('NUMBER_GREATER', ['15']),
                    format=cellFormat(backgroundColor=color(1, 0.9, 0.6), textFormat=textFormat(bold=True))
                )
            ))

        # 2. 分开处理：火箭 (🚀) 巅峰突破 - 红色高亮
        rules.append(ConditionalFormatRule(
            ranges=[GridRange.from_a1_range('B4:B100', ws)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('TEXT_CONTAINS', ['🚀']),
                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0, 0)))
            )
        ))

        # 3. 分开处理：火焰 (🔥) 主升浪 - 红色高亮
        rules.append(ConditionalFormatRule(
            ranges=[GridRange.from_a1_range('B4:B100', ws)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('TEXT_CONTAINS', ['🔥']),
                format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0, 0)))
            )
        ))

        # 4. 奇点先行 (👁️) - 紫色背景 (原有逻辑)
        rules.append(ConditionalFormatRule(
            ranges=[GridRange.from_a1_range('B4:B100', ws)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('TEXT_CONTAINS', ['👁️']),
                format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True))
            )
        ))

        # 5. 针对极度延伸 (☠️) - 灰色删除线 (针对 V750)
        rules.append(ConditionalFormatRule(
            ranges=[GridRange.from_a1_range('B4:B100', ws)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('TEXT_CONTAINS', ['☠️']),
                format=cellFormat(backgroundColor=color(0.85, 0.85, 0.85), 
                                 textFormat=textFormat(strikethrough=True, foregroundColor=color(0.5, 0.5, 0.5)))
            )
        ))

        rules.save()
