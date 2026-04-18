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
CORE_LEADERS =["NVDA", "AAPL", "MSFT", "TSLA", "META", "GOOGL", "AMZN", "NFLX", "PLTR", "AVGO", "COST"]

# 核心池硬编码字典 (防止不在标普500列表中的股票找不到行业)
CORE_INDS = {
    "NVDA": "Semiconductors", "AAPL": "Technology Hardware", "MSFT": "Systems Software",
    "TSLA": "Automobile Manufacturers", "META": "Interactive Media & Services",
    "GOOGL": "Interactive Media & Services", "AMZN": "Broadline Retail",
    "NFLX": "Movies & Entertainment", "PLTR": "Application Software",
    "AVGO": "Semiconductors", "COST": "Consumer Staples"
}

# ==========================================
# 🛡️ 核心 V750 巅峰引擎
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
# 3. 终极视觉输出引擎
# ==========================================
def final_output(results, vix, breadth):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        
        sh.format("A1:Q60", {"backgroundColor": {"red": 1, "green": 1, "blue": 1}, "textFormat": {"foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 10}, "horizontalAlignment": "CENTER"})

        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header = [
            ["🏰[V12.0 破壁 - 百分百共振版]", "", "Update(BJ):", bj_time],["市场天气:", "☀️" if vix < 20 else "☁️", "宽度(50MA):", f"{breadth:.1f}%", "VIX指数:", str(round(vix, 2))],["策略说明:", "🚀爆发 / 🌀VCP / 💎核心 / ⚔️反包", "共振说明:", "Resonance ≥ 3 为主线热点 (红色)"]
        ]
        sh.update(values=header, range_name="A1")

        if not results: return
        df = pd.DataFrame(results)
        cols_order =["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
        
        data_rows = [cols_order]
        for _, row in df.iterrows():
            r =[]
            for c in cols_order:
                val = row.get(c, "")
                if c in["ADR", "Bias", "5D", "20D", "60D", "R20", "R60"]: r.append(f"{float(val)*100:.2f}%")
                elif c == "Price": r.append(f"${float(val):.2f}")
                elif c in ["Score", "Vol_Ratio"]: r.append(str(round(float(val), 2)))
                elif c == "Resonance": r.append(str(int(val))) 
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
            
            if "🚀" in action_text:
                formats.append({"range": f"A{row_idx}:Q{row_idx}", "format": {"backgroundColor": {"red": 0.92, "green": 1, "blue": 0.92}}})
            elif "🌀" in action_text:
                formats.append({"range": f"A{row_idx}:Q{row_idx}", "format": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 1}}})
            
            if "🔥" in opt_text:
                formats.append({"range": f"K{row_idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}}})
                
            # 共振高亮
            if res_val >= 3:
                formats.append({"range": f"E{row_idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}}})
            elif res_val == 2:
                formats.append({"range": f"E{row_idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.5, "green": 0, "blue": 0.5}}}})
        
        if formats: sh.batch_format(formats)
        
        widths =[65, 170, 60, 110, 80, 75, 75, 70, 95, 75, 100, 85, 65, 65, 65, 65, 65]
        reqs =[{"updateDimensionProperties": {"range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}} for i, w in enumerate(widths)]
        client.open_by_key(SHEET_ID).batch_update({"requests": reqs})

        print(f"✅ V12.0 刷新成功！共振识别 100% 修复！")
    except Exception as e:
        print(f"❌ 输出报错: {e}")

# ==========================================
# 4. 执行流程
# ==========================================
def run_sentinel():
    print("📡 开启全量扫描 (V12.0)...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        # 1. 抓取维基百科，构建“不可战胜”的行业字典
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        sp_df['Symbol'] = sp_df['Symbol'].str.replace('.', '-')
        
        # 将 GICS Sub-Industry 直接做成映射字典 (内存级操作，绝不被墙)
        global_ind_dict = dict(zip(sp_df['Symbol'], sp_df['GICS Sub-Industry']))
        global_ind_dict.update(CORE_INDS) # 补充核心股
        
        tickers = list(set(list(global_ind_dict.keys()) + CORE_LEADERS))
        
        data = yf.download(tickers +["SPY", "^VIX"], period="2y", group_by='ticker', threads=True, progress=False)
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
        df_top = df_all.sort_values("Score", ascending=False).head(28)
        
        print("🏢 正在通过内存字典高速映射行业与共振...")
        
        final_list =[]
        for _, row in df_top.iterrows():
            t = row['Ticker']
            # 直接从内存字典拿行业，不走网络！
            ind = global_ind_dict.get(t, "Unknown")
            
            # 市值还是走 yfinance (这个允许失败，不影响共振)
            try: mkt = f"{yf.Ticker(t).info.get('marketCap', 0)/1e6:,.0f}"
            except: mkt = "N/A"
            
            d = row.to_dict()
            d.update({"Industry": ind, "MktCap": mkt})
            final_list.append(d)
        
        # --- 本地原生字典计数 (绝对不可能错) ---
        count_map = {}
        for item in final_list:
            ind = item['Industry']
            if ind != "Unknown":
                count_map[ind] = count_map.get(ind, 0) + 1
                
        print(f"📊 内存探测到的真实板块热点: {count_map}")
        
        # 赋值共振数
        for item in final_list:
            ind = item['Industry']
            if ind != "Unknown":
                item['Resonance'] = count_map[ind]
            else:
                item['Resonance'] = 1
                
        final_output(final_list, vix, (breadth_cnt/len(tickers)*100))
        
    except Exception as e:
        print(f"🚨 崩溃: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    run_sentinel()
