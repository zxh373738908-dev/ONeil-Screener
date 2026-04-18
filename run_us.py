import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import math
import traceback
import time

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json"
CORE_LEADERS =["NVDA", "AAPL", "MSFT", "TSLA", "META", "GOOGL", "AMZN", "NFLX", "PLTR", "AVGO", "COST"]

# ==========================================
# 🛡️ 核心引擎 (V11.0)
# ==========================================
def get_metrics(df, spy_df):
    try:
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        if len(close) < 150: return None
        curr = float(close.iloc[-1])
        
        adr_20 = float(((high - low) / low).tail(20).mean())
        adr_60 = float(((high - low) / low).tail(60).mean())
        vol_r = float(vol.iloc[-1] / vol.tail(20).mean())
        ma50 = float(close.rolling(50).mean().iloc[-1])
        
        # VCP 紧缩特征
        is_vcp = bool(adr_20 < adr_60 * 0.8)
        rs_raw = float((curr/close.iloc[-63])*2 + (curr/close.iloc[-126]) + (curr/close.iloc[-252]))
        
        action = "观察"
        if curr >= close.tail(126).max() * 0.98: action = "🚀 动量爆发"
        elif is_vcp and curr > ma50: action = "🌀 VCP紧缩"
        elif curr > ma50: action = "💎 核心趋势"
        elif vol_r > 2.0: action = "⚔️ 极速反包"

        options = "平稳"
        if vol_r > 2.8: options = "🔥 机构扫货"
        elif vol_r > 1.8: options = "👀 异动预警"

        return {
            "Price": curr, "Action": action, "Score": rs_raw, "ADR": adr_20,
            "Vol_Ratio": vol_r, "Bias": (curr-ma50)/ma50, "Options": options,
            "5D": float(curr/close.iloc[-5]-1), "20D": float(curr/close.iloc[-20]-1),
            "60D": float(curr/close.iloc[-60]-1),
            "R20": float(curr/close.iloc[-20]-1) - float(spy_df.iloc[-1]/spy_df.iloc[-20]-1),
            "R60": float(curr/close.iloc[-60]-1) - float(spy_df.iloc[-1]/spy_df.iloc[-60]-1),
            "RS_Raw": rs_raw
        }
    except: return None

# ==========================================
# 3. 终极输出 (V11.0)
# ==========================================
def final_output(df_final, vix, breadth):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        
        sh.format("A1:Q60", {"backgroundColor": {"red": 1, "green": 1, "blue": 1}, "textFormat": {"foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 10}, "horizontalAlignment": "CENTER"})

        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header = [
            ["🏰[V11.0 绝杀 - 强效共振版]", "", "Update(BJ):", bj_time],["市场天气:", "☀️" if vix < 20 else "☁️", "宽度(50MA):", f"{breadth:.1f}%", "VIX指数:", str(round(vix, 2))],["策略说明:", "🚀爆发 / 🌀VCP / 💎核心 / ⚔️反包", "共振说明:", "Resonance ≥ 3 为主线热点 (红色)"]
        ]
        sh.update(values=header, range_name="A1")

        if df_final.empty: return
        
        cols_order =["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
        
        data_rows = [cols_order]
        for _, row in df_final.iterrows():
            r =[]
            for c in cols_order:
                val = row.get(c, "")
                if c in["ADR", "Bias", "5D", "20D", "60D", "R20", "R60"]: r.append(f"{float(val)*100:.2f}%")
                elif c == "Price": r.append(f"${float(val):.2f}")
                elif c in ["Score", "Vol_Ratio"]: r.append(str(round(float(val), 2)))
                elif c == "Resonance": r.append(str(int(val))) # 强制整型转字符串
                else: r.append(str(val))
            data_rows.append(r)

        sh.update(values=data_rows, range_name="A5", value_input_option='USER_ENTERED')
        
        sh.format("A5:Q5", {"backgroundColor": {"red": 0.0, "green": 0.9, "blue": 0.0}, "textFormat": {"bold": True}})
        
        formats =[]
        for i in range(len(data_rows)-1):
            row_idx = i + 6
            action_text = data_rows[i+1][3]
            opt_text = data_rows[i+1][10]
            try: res_val = int(data_rows[i+1][4])
            except: res_val = 1
            
            # Action 涂色
            if "🚀" in action_text:
                formats.append({"range": f"A{row_idx}:Q{row_idx}", "format": {"backgroundColor": {"red": 0.92, "green": 1, "blue": 0.92}}})
            elif "🌀" in action_text:
                formats.append({"range": f"A{row_idx}:Q{row_idx}", "format": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 1}}})
            
            # 期权异动
            if "🔥" in opt_text:
                formats.append({"range": f"K{row_idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}}})
                
            # 阶梯式共振高亮
            if res_val >= 3:
                formats.append({"range": f"E{row_idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}}})
            elif res_val == 2:
                formats.append({"range": f"E{row_idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.5, "green": 0, "blue": 0.5}}}})
        
        if formats: sh.batch_format(formats)
        
        widths =[65, 170, 60, 110, 80, 75, 75, 70, 95, 75, 100, 85, 65, 65, 65, 65, 65]
        reqs =[{"updateDimensionProperties": {"range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}} for i, w in enumerate(widths)]
        client.open_by_key(SHEET_ID).batch_update({"requests": reqs})

        print(f"✅ V11.0 刷新成功！共振识别已彻底修复。")
    except Exception as e:
        print(f"❌ 输出报错: {e}")

# ==========================================
# 4. 执行流程
# ==========================================
def run_sentinel():
    print("📡 开启全量扫描 (V11.0)...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        tickers = list(set(tickers + CORE_LEADERS))
        
        data = yf.download(tickers + ["SPY", "^VIX"], period="2y", group_by='ticker', threads=True, progress=False)
        spy_df = data["SPY"]["Close"].dropna()
        vix = float(data["^VIX"]["Close"].iloc[-1])
        
        candidates =[]
        breadth_cnt = 0
        for t in tickers:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if len(df_t) < 150: continue
            if df_t['Close'].iloc[-1] > df_t['Close'].rolling(50).mean().iloc[-1]: breadth_cnt += 1
            
            m = get_metrics(df_t, spy_df)
            if m:
                m['Ticker'] = t
                candidates.append(m)
        
        if not candidates: return

        df_all = pd.DataFrame(candidates)
        df_all['RS_Rank'] = df_all['RS_Raw'].rank(pct=True).apply(lambda x: int(x * 99))
        df_top = df_all.sort_values("Score", ascending=False).head(28).copy()
        
        print("🏢 正在抓取并清洗行业数据 (使用底层 Pandas 聚类)...")
        
        # 1. 初始化空列
        df_top['Industry'] = "N/A"
        df_top['MktCap'] = "0"
        
        # 2. 逐一获取信息
        for idx, row in df_top.iterrows():
            t = row['Ticker']
            try:
                inf = yf.Ticker(t).info
                raw_ind = str(inf.get('industry', 'N/A'))
                # 核心杀招：去两端空格，全部首字母大写
                ind_clean = raw_ind.strip().title()
                df_top.at[idx, 'Industry'] = ind_clean
                df_top.at[idx, 'MktCap'] = f"{inf.get('marketCap', 0)/1e6:,.0f}"
                time.sleep(0.05)
            except: pass

        # 3. PANDAS 原生统计，直接利用 transform('count') 绝对不可能错
        df_top['Resonance'] = df_top.groupby('Industry')['Industry'].transform('count')
        
        # 屏蔽 N/A 的共振
        df_top.loc[df_top['Industry'] == 'N/A', 'Resonance'] = 1
        
        # --- 终端打印预览：让你直接看到有没有成功！ ---
        print("\n📊 === 当前提取的共振数据预览 ===")
