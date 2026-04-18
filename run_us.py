import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import math
import traceback

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json"
CORE_LEADERS = ["NVDA", "GOOGL", "CF", "PR", "TSLA", "PLTR", "META", "AVGO", "COST", "AAPL", "MSFT", "AMZN"]

# ==========================================
# 🛡️ 核心计算引擎
# ==========================================
def calculate_v750_apex_engine(df, spy_df):
    try:
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        if len(close) < 130: return None
        
        curr = close.iloc[-1]
        
        # 1. 基础涨幅与相对强度
        chg_5d = (curr / close.iloc[-5] - 1)
        chg_20d = (curr / close.iloc[-20] - 1)
        chg_60d = (curr / close.iloc[-60] - 1)
        spy_chg_20 = (spy_df.iloc[-1] / spy_df.iloc[-20] - 1)
        spy_chg_60 = (spy_df.iloc[-1] / spy_df.iloc[-60] - 1)
        rel_20, rel_60 = chg_20d - spy_chg_20, chg_60d - spy_chg_60
        
        # 2. 技术指标
        adr_20 = ((high - low) / low).tail(20).mean()
        ma20 = close.rolling(20).mean().iloc[-1]
        bias_20 = (curr - ma20) / ma20
        avg_vol_20 = vol.tail(20).mean()
        vol_ratio = vol.iloc[-1] / avg_vol_20 if avg_vol_20 > 0 else 0
        
        # 3. RS Score (IBD加权算法)
        rs_raw = ( (curr/close.iloc[-63])*2 + (curr/close.iloc[-126]) + (curr/close.iloc[-189]) + (curr/close.iloc[-252]) )
        
        # 4. 趋势判断
        ma50, ma200 = close.rolling(50).mean().iloc[-1], close.rolling(200).mean().iloc[-1]
        ema10 = close.ewm(span=10, adjust=False).mean().iloc[-1]
        rs_line = (close / spy_df).ffill()
        rs_nh = rs_line.iloc[-1] >= rs_line.tail(60).max()
        
        action = "观察"
        if rs_nh and curr >= close.tail(126).max() * 0.98: 
            action = "🚀 动量爆发"
        elif curr > ma50 > ma200 and rs_nh: 
            action = "💎 核心趋势"
        elif curr > ema10 and low.iloc[-1] < ema10 and (curr - low.iloc[-1])/(high.iloc[-1]-low.iloc[-1]) > 0.5:
            action = "⚔️ 极速反包"

        # 5. 期权异动/异常放量逻辑
        options_activity = "平稳"
        if vol_ratio > 3.0: options_activity = "🔥 期权激增"
        elif vol_ratio > 2.0: options_activity = "👀 异动预警"
        elif vol_ratio > 1.5 and abs(chg_5d) > 0.08: options_activity = "⚠️ 波动扩增"

        return {
            "Price": curr, "Action": action, "Score": rs_raw, "ADR": adr_20,
            "Vol_Ratio": vol_ratio, "Bias": bias_20, "Options": options_activity,
            "Chg_5D": chg_5d, "Chg_20D": chg_20d, "Chg_60D": chg_60d,
            "REL_20": rel_20, "REL_60": rel_60, "RS_Raw": rs_raw
        }
    except: return None

# ==========================================
# 3. 终极视觉输出引擎 (V9.0 - 亮绿表头版)
# ==========================================
def final_output(results, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        # --- 顶部信息栏 ---
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header_info = [
            ["🏰 [V750 巅峰 9.0 - 期权监控版]", "", "Update(BJ):", bj_time],
            ["市场天气:", weather, "宽度(50MA):", f"{breadth:.1f}%", "VIX指数:", round(vix, 2)],
            ["策略核心:", "趋势+强度+期权异动", "状态说明:", "🚀爆发 / 💎核心 / ⚔️反包 / 🟥高空"]
        ]
        sh.update(values=header_info, range_name="A1")
        
        if not results: return
        df_final = pd.DataFrame(results)
        
        # 整理列顺序
        display_cols = [
            "Ticker", "Industry", "Score", "Action", "Resonance", "ADR_20", 
            "Vol_Ratio", "Bias_20", "MktCap(M)", "RS_Rank", "Options", "Price", 
            "5D%", "20D%", "60D%", "REL_20", "REL_60"
        ]
        
        df_show = df_final[display_cols].copy()
        # 格式化数据映射
        fmt_map = {
            "Score": lambda x: round(x, 2), "ADR_20": lambda x: f"{x*100:.2f}%",
            "Vol_Ratio": lambda x: f"{x:.2f}", "Bias_20": lambda x: f"{x*100:.1f}%",
            "Price": lambda x: f"${x:.2f}", "5D%": lambda x: f"{x*100:.1f}%",
            "20D%": lambda x: f"{x*100:.1f}%", "60D%": lambda x: f"{x*100:.1f}%",
            "REL_20": lambda x: f"{x*100:.1f}%", "REL_60": lambda x: f"{x*100:.1f}%"
        }
        for col, func in fmt_map.items(): df_show[col] = df_show[col].apply(func)

        # 写入数据 (A5开始)
        sh.update(values=[df_show.columns.tolist()] + df_show.values.tolist(), range_name="A5")
        
        # --- 🎨 样式渲染 ---
        # 1. 亮绿色表头 (高对比度)
        sh.format("A5:Q5", {
            "backgroundColor": {"red": 0.0, "green": 0.9, "blue": 0.0}, # 亮绿色
            "textFormat": {"bold": True, "foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        })
        
        # 2. 全局基础
        sh.format("A6:Q50", {"horizontalAlignment": "CENTER", "textFormat": {"fontSize": 9}})
        
        # 3. 条件高亮
        formats = []
        for i, row in df_final.iterrows():
            idx = i + 6
            # 趋势行
            if "🚀" in row['Action']:
                formats.append({"range": f"A{idx}:Q{idx}", "format": {"backgroundColor": {"red": 0.9, "green": 1.0, "blue": 0.9}}})
            elif "⚔️" in row['Action']:
                formats.append({"range": f"A{idx}:Q{idx}", "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.85}}})
            
            # 期权异动标记红字
            if "🔥" in row['Options']:
                formats.append({"range": f"K{idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}}})

        if formats: sh.batch_format(formats)
        
        # 4. 列宽适配
        widths = [60, 130, 60, 100, 70, 70, 60, 60, 85, 60, 100, 75, 60, 60, 60, 60, 60]
        requests = [{"updateDimensionProperties": {"range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}} for i, w in enumerate(widths)]
        client.open_by_key(SHEET_ID).batch_update({"requests": requests})

        print(f"✅ 看板已刷新！表头已更新为亮绿色。")
    except Exception as e:
        print(f"❌ 输出失败: {e}")

# ==========================================
# 4. 执行流程
# ==========================================
def run_v750_apex_sentinel():
    print("📡 正在全量扫描标的...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        tickers = list(set(tickers + CORE_LEADERS))
        
        full_data = yf.download(tickers + ["SPY", "^VIX"], period="14mo", group_by='ticker', threads=True, progress=False)
        spy_df = full_data["SPY"]["Close"].dropna()
        vix = full_data["^VIX"]["Close"].iloc[-1]
        
        all_candidates = []
        breadth_count = 0
        
        for t in tickers:
            if t not in full_data.columns.levels[0]: continue
            df_t = full_data[t].dropna()
            if len(df_t) < 250: continue
            
            if df_t['Close'].iloc[-1] > df_t['Close'].rolling(50).mean().iloc[-1]: breadth_count += 1
            
            metrics = calculate_v750_apex_engine(df_t, spy_df)
            if metrics and metrics['Action'] != "观察":
                metrics['Ticker'] = t
                all_candidates.append(metrics)
        
        if not all_candidates: return
        
        df_calc = pd.DataFrame(all_candidates)
        df_calc['RS_Rank'] = df_calc['RS_Raw'].rank(pct=True).apply(lambda x: int(x * 99))
        final_list = df_calc.sort_values("Score", ascending=False).head(28)
        
        # 补充行业信息
        processed = []
        ind_counts = {}
        for _, row in final_list.iterrows():
            t = row['Ticker']
            try:
                info = yf.Ticker(t).info
                ind, mkt = info.get('industry', 'N/A'), info.get('marketCap', 0) / 1_000_000
                ind_counts[ind] = ind_counts.get(ind, 0) + 1
            except: ind, mkt = "N/A", 0
            
            d = row.to_dict()
            d.update({"Industry": ind, "MktCap(M)": f"{mkt:,.0f}"})
            processed.append(d)
            
        for item in processed: item['Resonance'] = ind_counts.get(item['Industry'], 0)
        
        market_breadth = (breadth_count / len(tickers)) * 100
        weather = "☀️" if market_breadth > 60 and vix < 20 else "☁️"
        final_output(processed, vix, market_breadth, weather)

    except Exception as e:
        print(f"🚨 崩溃: {e}"); traceback.print_exc()

if __name__ == "__main__":
    run_v750_apex_sentinel()
