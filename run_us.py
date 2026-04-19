import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
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
        
        # --- 🌟 新增计算：从 2025-12-31 至今的涨幅 ---
        try:
            # 截取到 2025-12-31 的历史数据，取最后一天作为基准价
            hist_closes = close.loc[:'2025-12-31']
            if len(hist_closes) > 0:
                ret_251231 = float(curr / hist_closes.iloc[-1] - 1)
            else:
                ret_251231 = 0.0
        except:
            ret_251231 = 0.0
        # ---------------------------------------------
        
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
            "From 2025-12-31": ret_251231,  # <--- 增加此列
            "5D": float(curr/close.iloc[-5]-1), "20D": float(curr/close.iloc[-20]-1),
            "60D": float(curr/close.iloc[-60]-1),
            "R20": float(curr/close.iloc[-20]-1) - float(spy_df.iloc[-1]/spy_df.iloc[-20]-1),
            "R60": float(curr/close.iloc[-60]-1) - float(spy_df.iloc[-1]/spy_df.iloc[-60]-1),
            "RS_Raw": rs_raw
        }
    except: return None

# ==========================================
# 3. 终极视觉输出引擎 (V21.0 强制刷新版)
# ==========================================
def final_output(final_results_list, vix, breadth):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        
        # 1. 暴力初始化：清空一切内容和格式 (范围从 Q 扩展到 R，适应新增列)
        sh.clear()
        sh.format("A1:R60", {"backgroundColor": {"red": 1, "green": 1, "blue": 1}, "textFormat": {"foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 10}, "horizontalAlignment": "CENTER"})

        # 2. 准备表头时间
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header =[
            ["🏰[V21.0 终极共振对齐版]", "", "", "更新时间(BJ):", bj_time],["市场天气:", "☀️" if vix < 20 else "☁️", "", "全美宽度:", f"{breadth:.1f}%", "VIX指数:", str(round(vix, 2))],["策略雷达:", "🚀爆发 / 🌀VCP / 💎核心", "", "共振说明:", "≥3 红色 / =2 紫色"]
        ]
        sh.update(values=header, range_name="A1")
        sh.format("A1:A3", {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}})

        if not final_results_list: return

        # 3. 构造数据矩阵 (加入 "From 2025-12-31" 字段)
        cols_order =["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", "Options", "Price", "From 2025-12-31", "5D", "20D", "60D", "R20", "R60"]
        data_rows =[cols_order]
        
        for item in final_results_list:
            row_data =[]
            for col in cols_order:
                val = item.get(col, "")
                # 将新增列也加入百分比格式化
                if col in["ADR", "Bias", "From 2025-12-31", "5D", "20D", "60D", "R20", "R60"]:
                    row_data.append(f"{float(val)*100:.2f}%")
                elif col == "Price":
                    row_data.append(f"${float(val):.2f}")
                elif col in ["Score", "Vol_Ratio"]:
                    row_data.append(str(round(float(val), 2)))
                elif col == "Resonance":
                    row_data.append(str(int(val)))
                else:
                    row_data.append(str(val))
            data_rows.append(row_data)

        # 4. 一次性写入数据
        sh.update(values=data_rows, range_name="A5", value_input_option='USER_ENTERED')
        
        # 5. 渲染样式 (标题栏扩展至 R 列)
        sh.format("A5:R5", {"backgroundColor": {"red": 0.0, "green": 0.9, "blue": 0.0}, "textFormat": {"bold": True}})
        
        formats =[]
        for i in range(len(data_rows)-1):
            row_idx = i + 6
            action_txt = data_rows[i+1][3]
            opt_txt = data_rows[i+1][10]
            try: r_val = int(data_rows[i+1][4])
            except: r_val = 1
            
            # 行高亮同样扩展至 R 列
            if "🚀" in action_txt:
                formats.append({"range": f"A{row_idx}:R{row_idx}", "format": {"backgroundColor": {"red": 0.92, "green": 1, "blue": 0.92}}})
            elif "🌀" in action_txt:
                formats.append({"range": f"A{row_idx}:R{row_idx}", "format": {"backgroundColor": {"red": 0.9, "green": 0.95, "blue": 1}}})
            
            if r_val >= 3:
                formats.append({"range": f"E{row_idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}}})
            elif r_val == 2:
                formats.append({"range": f"E{row_idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.5, "green": 0, "blue": 0.5}}}})
        
        if formats: sh.batch_format(formats)
        print(f"✨ 表格已成功更新至 V21.0 (含YTD数据)，共振数已同步。")
    except Exception as e:
        print(f"❌ 写入报错: {e}")
        traceback.print_exc()

# ==========================================
# 4. 主执行流程
# ==========================================
def run_sentinel():
    print("📡 开启全美股扫描 (V21.0)...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        tickers = list(set(tickers + CORE_LEADERS))
        
        data = yf.download(tickers + ["SPY", "^VIX"], period="2y", group_by='ticker', threads=False, progress=False)
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
        
        # 计算 RS Rank
        df_all = pd.DataFrame(candidates)
        df_all['RS_Rank'] = df_all['RS_Raw'].rank(pct=True).apply(lambda x: int(x * 99))
        df_top = df_all.sort_values("Score", ascending=False).head(28)
        
        # 抓取行业并计算共振
        final_list =[]
        print("🏢 抓取行业信息并计算共振...")
        for _, row in df_top.iterrows():
            t = row['Ticker']
            try:
                inf = yf.Ticker(t).info
                ind = str(inf.get('industry', 'N/A')).strip()
                mkt = f"{inf.get('marketCap', 0)/1e6:,.0f}"
            except: ind, mkt = "N/A", "0"
            
            d = row.to_dict()
            d['Industry'] = ind
            d['MktCap'] = mkt
            final_list.append(d)
        
        # 暴力计算共振
        all_inds = [x['Industry'] for x in final_list if x['Industry'] != "N/A"]
        for item in final_list:
            item['Resonance'] = all_inds.count(item['Industry']) if item['Industry'] != "N/A" else 1
            print(f"{item['Ticker']} | {item['Industry']} | Res: {item['Resonance']}")

        # 最后的输出
        final_output(final_list, vix, (breadth_cnt/len(tickers)*100))
        
    except Exception as e:
        print(f"🚨 崩溃: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    run_sentinel()
