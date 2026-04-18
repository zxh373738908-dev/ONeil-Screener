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
# 🛡️ 核心 VCP & 引擎逻辑
# ==========================================
def calculate_v750_apex_engine(df, spy_df):
    try:
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        if len(close) < 150: return None
        
        curr = float(close.iloc[-1])
        chg_5d = float(curr / close.iloc[-5] - 1)
        chg_20d = float(curr / close.iloc[-20] - 1)
        spy_chg_20 = float(spy_df.iloc[-1] / spy_df.iloc[-20] - 1)
        rel_20 = chg_20d - spy_chg_20
        
        adr_20 = float(((high - low) / low).tail(20).mean())
        adr_60 = float(((high - low) / low).tail(60).mean())
        avg_vol_20 = float(vol.tail(20).mean())
        vol_ratio = float(vol.iloc[-1] / avg_vol_20 if avg_vol_20 > 0 else 0)
        
        # RS Score
        rs_raw = float((curr/close.iloc[-63])*2 + (curr/close.iloc[-126]) + (curr/close.iloc[-189]) + (curr/close.iloc[-252]))
        
        rs_line = (close / spy_df).ffill()
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(60).max())
        is_tight = bool(adr_20 < adr_60 * 0.85)
        
        action = "观察"
        if rs_nh and curr >= close.tail(126).max() * 0.98: action = "🚀 动量爆发"
        elif is_tight and curr > close.rolling(50).mean().iloc[-1]: action = "🌀 VCP紧缩"
        elif rs_nh: action = "💎 核心趋势"
        elif vol_ratio > 2.0 and (curr > low.iloc[-1]): action = "⚔️ 极速反包"

        opt_status = "平稳"
        if vol_ratio > 3.0: opt_status = "🔥 机构扫货"
        elif vol_ratio > 2.0: opt_status = "👀 异动预警"

        return {
            "Price": curr, "Action": action, "Score": rs_raw, "ADR": adr_20,
            "Vol_Ratio": vol_ratio, "Options": opt_status,
            "Chg_5D": chg_5d, "Chg_20D": chg_20d, "REL_20": rel_20, "RS_Raw": rs_raw
        }
    except: return None

# ==========================================
# 3. 视觉输出引擎 (V9.6 修正数据消失问题)
# ==========================================
def final_output(results, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        
        # 强制清空所有单元格和格式
        sh.clear()
        
        # 1. 顶部状态栏
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header_status = [
            ["🏰 [V750 巅峰 9.6 - 稳定输出版]", "", "更新时间(北京):", bj_time],
            ["当前天气:", weather, "全美宽度(50MA):", f"{breadth:.1f}%", "VIX恐惧指数:", round(vix, 2)],
            ["策略核心:", "RS强度 + VCP紧缩 + 期权爆发", "状态说明:", "🚀爆发 / 💎核心 / 🌀紧缩 / ⚔️反包"]
        ]
        sh.update(values=header_status, range_name="A1")
        
        if not results:
            sh.update(values=[["⚠️ 当前暂无符合筛选条件的形态个股，请等待市场机会"]], range_name="A5")
            return

        # 2. 数据处理
        df_final = pd.DataFrame(results)
        display_cols = [
            "Ticker", "Industry", "Score", "Action", "Resonance", "ADR_20", 
            "Vol_Ratio", "MktCap(M)", "RS_Rank", "Options", "Price", 
            "5D%", "20D%", "REL_20"
        ]
        
        # 格式化并强制转为字符串，确保一定能显示
        df_show = df_final[display_cols].copy()
        df_show["Score"] = df_show["Score"].apply(lambda x: str(round(x, 2)))
        df_show["ADR_20"] = df_show["ADR_20"].apply(lambda x: f"{x*100:.2f}%")
        df_show["Vol_Ratio"] = df_show["Vol_Ratio"].apply(lambda x: f"{x:.2f}")
        df_show["5D%"] = df_show["5D%"].apply(lambda x: f"{x*100:.1f}%")
        df_show["20D%"] = df_show["20D%"].apply(lambda x: f"{x*100:.1f}%")
        df_show["REL_20"] = df_show["REL_20"].apply(lambda x: f"{x*100:.1f}%")
        df_show["Price"] = df_show["Price"].apply(lambda x: f"${x:.2f}")
        df_show["RS_Rank"] = df_show["RS_Rank"].astype(str)

        # 转换为列表套列表 (这是 gspread 写入的最稳妥方式)
        data_to_write = [df_show.columns.tolist()] + df_show.values.tolist()
        
        # 写入表格数据
        sh.update(values=data_to_write, range_name="A5")
        
        # 3. 样式美化
        # 表头：亮绿色 + 黑色加粗
        sh.format("A5:N5", {
            "backgroundColor": {"red": 0.0, "green": 0.9, "blue": 0.0},
            "textFormat": {"bold": True, "foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        })
        
        # 数据行：基础格式
        sh.format(f"A6:N{len(data_to_write)+5}", {
            "horizontalAlignment": "CENTER", 
            "textFormat": {"fontSize": 9, "foregroundColor": {"red": 0, "green": 0, "blue": 0}}
        })

        # 条件涂色
        formats = []
        for i, row in df_final.iterrows():
            idx = i + 6
            if "🚀" in row['Action']:
                formats.append({"range": f"A{idx}:N{idx}", "format": {"backgroundColor": {"red": 0.9, "green": 1.0, "blue": 0.9}}})
            elif "⚔️" in row['Action']:
                formats.append({"range": f"A{idx}:N{idx}", "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.85}}})
            if "🔥" in str(row['Options']):
                formats.append({"range": f"J{idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}}})

        if formats: sh.batch_format(formats)
        
        # 列宽
        widths = [60, 130, 60, 100, 75, 75, 65, 85, 65, 100, 75, 65, 65, 65]
        requests = [{"updateDimensionProperties": {"range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}} for i, w in enumerate(widths)]
        client.open_by_key(SHEET_ID).batch_update({"requests": requests})

        print(f"✅ 审计看板刷新成功！数据已强制同步。")
    except Exception as e:
        print(f"❌ 输出失败: {e}")
        traceback.print_exc()

# ==========================================
# 4. 执行流程
# ==========================================
def run_v750_apex_sentinel():
    print("📡 正在扫描全美股标的...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        # 尝试抓取标的，如果失败使用核心池
        try:
            tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        except:
            tickers = CORE_LEADERS
        tickers = list(set(tickers + CORE_LEADERS))
        
        data = yf.download(tickers + ["SPY", "^VIX"], period="14mo", group_by='ticker', threads=True, progress=False)
        spy_df = data["SPY"]["Close"].dropna()
        vix = float(data["^VIX"]["Close"].iloc[-1])
        
        results_raw = []
        breadth_count = 0
        
        for t in tickers:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if len(df_t) < 250: continue
            
            if df_t['Close'].iloc[-1] > df_t['Close'].rolling(50).mean().iloc[-1]: breadth_count += 1
            
            res = calculate_v750_apex_engine(df_t, spy_df)
            if res and res['Action'] != "观察":
                res['Ticker'] = t
                results_raw.append(res)
        
        if not results_raw:
            final_output([], vix, (breadth_count/len(tickers)*100), "☁️")
            return
        
        # 计算 RS Rank
        df_all = pd.DataFrame(results_raw)
        df_all['RS_Rank'] = df_all['RS_Raw'].rank(pct=True).apply(lambda x: int(x * 99))
        final_list = df_all.sort_values("Score", ascending=False).head(28)
        
        # 补充行业信息
        processed = []
        ind_counts = {}
        for _, row in final_list.iterrows():
            t = row['Ticker']
            try:
                inf = yf.Ticker(t).info
                ind, mkt = inf.get('industry', 'N/A'), inf.get('marketCap', 0) / 1_000_000
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
