import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import time
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
# 🛡️ 核心工具
# ==========================================
def safe_div(n, d):
    try:
        n_f, d_f = float(n), float(d)
        return n_f / d_f if d_f != 0 and math.isfinite(n_f) and math.isfinite(d_f) else 0.0
    except: return 0.0

# ==========================================
# 2. V750 巅峰引擎 (增强版)
# ==========================================
def calculate_v750_apex_engine(df, spy_df):
    try:
        close = df['Close']
        high = df['High']
        low = df['Low']
        vol = df['Volume']
        
        if len(close) < 130: return None
        curr = close.iloc[-1]
        
        # 1. 基础涨幅
        chg_5d = (curr / close.iloc[-5] - 1)
        chg_20d = (curr / close.iloc[-20] - 1)
        chg_60d = (curr / close.iloc[-60] - 1)
        
        # 2. 相对强度 (REL)
        spy_chg_20 = (spy_df.iloc[-1] / spy_df.iloc[-20] - 1)
        spy_chg_60 = (spy_df.iloc[-1] / spy_df.iloc[-60] - 1)
        rel_20 = chg_20d - spy_chg_20
        rel_60 = chg_60d - spy_chg_60
        
        # 3. 技术指标
        adr_20 = ((high - low) / low).tail(20).mean()
        ma20 = close.rolling(20).mean().iloc[-1]
        bias_20 = (curr - ma20) / ma20
        avg_vol_20 = vol.tail(20).mean()
        vol_ratio = vol.iloc[-1] / avg_vol_20 if avg_vol_20 > 0 else 0
        
        # 4. IBD RS Score 计算 (加权)
        rs_raw = ( (curr/close.iloc[-63])*2 + (curr/close.iloc[-126]) + (curr/close.iloc[-189]) + (curr/close.iloc[-252]) )
        
        # 5. 趋势状态判断
        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1]
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

        # 6. 期权异动初步逻辑：成交量突发且处于关键状态
        opt_status = "平稳"
        if vol_ratio > 2.5: opt_status = "🔥 交易量激增"
        elif vol_ratio > 1.8 and abs(chg_5d) > 0.05: opt_status = "👀 异动预警"

        return {
            "Price": curr,
            "Action": action,
            "Score": rs_raw,
            "ADR": adr_20,
            "Vol_Ratio": vol_ratio,
            "Bias": bias_20,
            "Options": opt_status,
            "Chg_5D": chg_5d,
            "Chg_20D": chg_20d,
            "Chg_60D": chg_60d,
            "REL_20": rel_20,
            "REL_60": rel_60,
            "RS_Raw": rs_raw
        }
    except: return None

# ==========================================
# 3. 终极视觉输出引擎 (V8.9 - 修正色彩版)
# ==========================================
def final_output(results, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        # 标题栏设置
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header = [
            ["🏰 [V750 巅峰 8.9 - 视觉增强版]", "", "Update(BJ):", bj_time],
            ["市场天气:", weather, "宽度(50MA):", f"{breadth:.1f}%", "VIX指数:", round(vix, 2)],
            ["策略核心:", "趋势+强度+期权异动", "状态说明:", "🚀爆发 / 💎核心 / ⚔️反包 / 🟥高空"]
        ]
        sh.update(values=header, range_name="A1")
        
        if not results: return

        df_final = pd.DataFrame(results)
        
        # 定义列顺序 (包含期权异动)
        display_cols = [
            "Ticker", "Industry", "Score", "Action", "Resonance", "ADR_20", 
            "Vol_Ratio", "Bias_20", "MktCap(M)", "RS_Rank", "Options", "Price", 
            "5D%", "20D%", "60D%", "REL_20", "REL_60"
        ]
        
        df_show = df_final[display_cols].copy()
        # 格式化
        df_show["Score"] = df_show["Score"].apply(lambda x: round(x, 2))
        df_show["ADR_20"] = df_show["ADR_20"].apply(lambda x: f"{x*100:.2f}%")
        df_show["Vol_Ratio"] = df_show["Vol_Ratio"].apply(lambda x: f"{x:.2f}")
        df_show["Bias_20"] = df_show["Bias_20"].apply(lambda x: f"{x*100:.1f}%")
        df_show["5D%"] = df_show["5D%"].apply(lambda x: f"{x*100:.1f}%")
        df_show["20D%"] = df_show["20D%"].apply(lambda x: f"{x*100:.1f}%")
        df_show["60D%"] = df_show["60D%"].apply(lambda x: f"{x*100:.1f}%")
        df_show["REL_20"] = df_show["REL_20"].apply(lambda x: f"{x*100:.1f}%")
        df_show["REL_60"] = df_show["REL_60"].apply(lambda x: f"{x*100:.1f}%")
        df_show["Price"] = df_show["Price"].apply(lambda x: f"${x:.2f}")

        # 写入表格
        sh.update(values=[df_show.columns.tolist()] + df_show.values.tolist(), range_name="A5")
        
        # --- 重点：格式化美化 ---
        # 1. 表头：改为浅蓝色背景，黑字
        sh.format("A5:Q5", {
            "backgroundColor": {"red": 0.8, "green": 0.9, "blue": 1.0}, # 浅蓝
            "textFormat": {"bold": True, "foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        })
        
        # 2. 全局基础格式
        sh.format("A6:Q50", {"horizontalAlignment": "CENTER", "textFormat": {"fontSize": 9}})
        
        # 3. 批量涂色
        formats = []
        for i, row in df_final.iterrows():
            idx = i + 6
            # 趋势状态背景色
            if "🚀" in row['Action'] or "💎" in row['Action']:
                formats.append({"range": f"A{idx}:Q{idx}", "format": {"backgroundColor": {"red": 0.95, "green": 1.0, "blue": 0.95}}}) # 极淡绿
            elif "⚔️" in row['Action']:
                formats.append({"range": f"A{idx}:Q{idx}", "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.9}}}) # 极淡黄
            
            # 期权异动加亮
            if "🔥" in row['Options']:
                formats.append({"range": f"K{idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.9, "green": 0.1, "blue": 0.1}}}})

        if formats: sh.batch_format(formats)
        
        # 4. 自动调整列宽
        widths = [60, 130, 60, 100, 70, 70, 60, 60, 85, 60, 100, 70, 60, 60, 60, 60, 60]
        requests = [{"updateDimensionProperties": {"range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}} for i, w in enumerate(widths)]
        client.open_by_key(SHEET_ID).batch_update({"requests": requests})

        print(f"✅ 刷新成功! 表头已改为高对比度浅色样式。")
    except Exception as e:
        print(f"❌ 输出错误: {e}")

# ==========================================
# 4. 主执行逻辑
# ==========================================
def run_v750_apex_sentinel():
    print("📡 开启全域扫描...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        tickers = list(set(tickers + CORE_LEADERS))
        
        full_data = yf.download(tickers + ["SPY", "^VIX"], period="14mo", group_by='ticker', threads=True, progress=False)
        spy_df = full_data["SPY"]["Close"].dropna()
        vix = full_data["^VIX"]["Close"].iloc[-1]
        
        breadth_count = 0
        all_candidates = []
        
        for t in tickers:
            if t not in full_data.columns.levels[0]: continue
            df_t = full_data[t].dropna()
            if len(df_t) < 250: continue
            
            # 市场宽度计算
            ma50 = df_t['Close'].rolling(50).mean().iloc[-1]
            if df_t['Close'].iloc[-1] > ma50: breadth_count += 1
            
            # 指标引擎
            metrics = calculate_v750_apex_engine(df_t, spy_df)
            if metrics and metrics['Action'] != "观察":
                metrics['Ticker'] = t
                all_candidates.append(metrics)
        
        if not all_candidates: 
            print("未发现符合形态的个股"); return
        
        # 计算 RS Rank
        df_calc = pd.DataFrame(all_candidates)
        df_calc['RS_Rank'] = df_calc['RS_Raw'].rank(pct=True).apply(lambda x: int(x * 99))
        
        # 截取前 25 只最强股
        final_list = df_calc.sort_values("Score", ascending=False).head(25)
        
        # 补充行业信息
        industry_counts = {}
        processed_results = []
        for _, row in final_list.iterrows():
            t = row['Ticker']
            try:
                info = yf.Ticker(t).info
                ind = info.get('industry', 'N/A')
                mkt = info.get('marketCap', 0) / 1_000_000
                industry_counts[ind] = industry_counts.get(ind, 0) + 1
            except: ind, mkt = "N/A", 0
            
            d = row.to_dict()
            d.update({"Industry": ind, "MktCap(M)": f"{mkt:,.0f}"})
            processed_results.append(d)
            
        for item in processed_results:
            item['Resonance'] = industry_counts.get(item['Industry'], 0)
        
        market_breadth = (breadth_count / len(tickers)) * 100
        weather = "☀️" if market_breadth > 60 and vix < 20 else "☁️"
        
        final_output(processed_results, vix, market_breadth, weather)

    except Exception as e:
        print(f"🚨 错误: {e}"); traceback.print_exc()

if __name__ == "__main__":
    run_v750_apex_sentinel()
