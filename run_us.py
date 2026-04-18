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
# 🛡️ 核心计算引擎 (强制返回原生 Python 类型)
# ==========================================
def calculate_v750_apex_engine(df, spy_df):
    try:
        close = df['Close']
        if len(close) < 150: return None
        
        curr = float(close.iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        
        # 基础计算
        adr_20 = float(((df['High'] - df['Low']) / df['Low']).tail(20).mean())
        adr_60 = float(((df['High'] - df['Low']) / df['Low']).tail(60).mean())
        vol_ratio = float(df['Volume'].iloc[-1] / df['Volume'].tail(20).mean())
        
        # RS Score (IBD 风格)
        rs_raw = float((curr/close.iloc[-63])*2 + (curr/close.iloc[-126]) + (curr/close.iloc[-189]) + (curr/close.iloc[-252]))
        
        # 趋势判断
        rs_line = (close / spy_df).ffill()
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(60).max())
        is_tight = bool(adr_20 < adr_60 * 0.9)
        
        action = "观察"
        if rs_nh and curr >= close.tail(126).max() * 0.97: action = "🚀 动量爆发"
        elif is_tight and curr > ma50: action = "🌀 VCP紧缩"
        elif rs_nh: action = "💎 核心趋势"
        elif vol_ratio > 1.8: action = "⚔️ 极速反包"

        # 只要不是"观察"，或者属于核心池，就入选
        return {
            "Price": curr, "Action": action, "Score": rs_raw, "ADR": adr_20,
            "Vol_Ratio": vol_ratio, "Options": "🔥 机构扫货" if vol_ratio > 2.8 else "平稳",
            "Chg_5D": float(curr / close.iloc[-5] - 1),
            "Chg_20D": float(curr / close.iloc[-20] - 1),
            "RS_Raw": rs_raw
        }
    except: return None

# ==========================================
# 3. 终极输出引擎 (解决“字不见了”专项优化)
# ==========================================
def final_output(results, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        
        # 第一步：彻底清空内容和所有格式
        sh.clear()
        
        # 1. 构造顶部信息 (Row 1-3)
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header_status = [
            ["🏰 [V750 巅峰 9.7 - 终极修复版]", "", "更新时间(北京):", bj_time],
            ["当前天气:", weather, "全美宽度(50MA):", f"{breadth:.1f}%", "VIX恐惧指数:", str(round(vix, 2))],
            ["策略核心:", "RS强度 + VCP紧缩 + 期权爆发", "状态说明:", "🚀爆发 / 💎核心 / 🌀紧缩 / ⚔️反包"]
        ]
        sh.update(range_name="A1", values=header_status)
        
        # 2. 检查数据
        if not results:
            sh.update(range_name="A5", values=[["⚠️ 提示：当前市场未扫到符合形态的股票，请关注大盘指数。"]])
            return

        df = pd.DataFrame(results)
        # 指定列顺序
        cols = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR_20", "Vol_Ratio", "MktCap(M)", "RS_Rank", "Options", "Price", "5D%", "20D%"]
        df = df[cols]

        # --- 关键：强制转为纯字符串列表，防止 Google Sheets 拒绝显示 ---
        def clean_val(v):
            if isinstance(v, float):
                if "Score" in str(v): return str(round(v, 2))
                return str(v)
            return str(v)

        final_rows = [df.columns.tolist()]
        for _, row in df.iterrows():
            clean_row = []
            for col in cols:
                val = row[col]
                # 针对性美化字符串
                if col in ["ADR_20", "5D%", "20D%"]: clean_row.append(f"{float(val)*100:.2f}%")
                elif col == "Price": clean_row.append(f"${float(val):.2f}")
                elif col == "Score": clean_row.append(str(round(float(val), 2)))
                elif col == "Vol_Ratio": clean_row.append(str(round(float(val), 2)))
                else: clean_row.append(str(val))
            final_rows.append(clean_row)

        print(f"📦 正在同步数据到表格: {len(final_rows)-1} 条记录")
        
        # 执行写入
        sh.update(range_name="A5", values=final_rows, value_input_option='USER_ENTERED')
        
        # --- 3. 样式美化 (强制文字为黑色) ---
        # 全局强制：黑色字, 9号字
        sh.format(f"A1:M{len(final_rows)+10}", {
            "textFormat": {"foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 9},
            "horizontalAlignment": "CENTER"
        })
        
        # 表头：亮绿色 + 黑色加粗
        sh.format("A5:M5", {
            "backgroundColor": {"red": 0.0, "green": 0.9, "blue": 0.0},
            "textFormat": {"bold": True, "foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 10}
        })
        
        # 条件背景涂色
        formats = []
        for i in range(len(final_rows)-1):
            row_idx = i + 6
            action_str = final_rows[i+1][3] # Action column
            if "🚀" in action_str:
                formats.append({"range": f"A{row_idx}:M{row_idx}", "format": {"backgroundColor": {"red": 0.92, "green": 1.0, "blue": 0.92}}})
            elif "⚔️" in action_str:
                formats.append({"range": f"A{row_idx}:M{row_idx}", "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.88}}})

        if formats: sh.batch_format(formats)

        # 自动调整列宽
        widths = [60, 130, 60, 100, 75, 75, 65, 85, 65, 100, 75, 65, 65]
        requests = [{"updateDimensionProperties": {"range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}} for i, w in enumerate(widths)]
        client.open_by_key(SHEET_ID).batch_update({"requests": requests})

        print(f"✨ 刷新完成！如果仍看不到字，请刷新浏览器页面。")
    except Exception as e:
        print(f"❌ 运行报错: {e}")
        traceback.print_exc()

# ==========================================
# 4. 主执行逻辑
# ==========================================
def run_v750_apex_sentinel():
    print("📡 正在全量获取市场数据...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        # 获取 S&P 500
        try:
            tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        except:
            tickers = CORE_LEADERS
            
        tickers = list(set(tickers + CORE_LEADERS))
        data = yf.download(tickers + ["SPY", "^VIX"], period="14mo", group_by='ticker', threads=True, progress=False)
        
        spy_df = data["SPY"]["Close"].dropna()
        vix = float(data["^VIX"]["Close"].iloc[-1])
        
        raw_list = []
        breadth_cnt = 0
        
        for t in tickers:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if len(df_t) < 250: continue
            
            # 市场宽度计算
            if df_t['Close'].iloc[-1] > df_t['Close'].rolling(50).mean().iloc[-1]: breadth_cnt += 1
            
            res = calculate_v750_apex_engine(df_t, spy_df)
            if res and res['Action'] != "观察":
                res['Ticker'] = t
                raw_list.append(res)
        
        if not raw_list:
            final_output([], vix, (breadth_cnt/len(tickers)*100), "☁️")
            return

        # 计算 RS Rank
        df_all = pd.DataFrame(raw_list)
        df_all['RS_Rank'] = df_all['RS_Raw'].rank(pct=True).apply(lambda x: int(x * 99))
        df_top = df_all.sort_values("Score", ascending=False).head(28)
        
        # 补充行业信息
        final_processed = []
        ind_counts = {}
        for _, row in df_top.iterrows():
            t = row['Ticker']
            try:
                inf = yf.Ticker(t).info
                ind, mkt = inf.get('industry', 'N/A'), inf.get('marketCap', 0) / 1_000_000
                ind_counts[ind] = ind_counts.get(ind, 0) + 1
            except: ind, mkt = "N/A", 0
            
            d = row.to_dict()
            d.update({"Industry": ind, "MktCap(M)": f"{mkt:,.0f}"})
            final_processed.append(d)
            
        for item in final_processed:
            item['Resonance'] = ind_counts.get(item['Industry'], 0)
        
        breadth_pct = (breadth_cnt / len(tickers)) * 100
        weather = "☀️" if breadth_pct > 60 and vix < 20 else "☁️"
        
        final_output(final_processed, vix, breadth_pct, weather)

    except Exception as e:
        print(f"🚨 程序崩溃: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    run_v750_apex_sentinel()
