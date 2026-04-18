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
# 核心观察池
CORE_LEADERS = ["NVDA", "GOOGL", "CF", "PR", "TSLA", "PLTR", "META", "AVGO", "COST", "AAPL", "MSFT", "AMZN"]

# ==========================================
# 🛡️ 核心 VCP & 期权异动引擎
# ==========================================
def calculate_v750_apex_engine(df, spy_df):
    try:
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        if len(close) < 150: return None
        
        curr = close.iloc[-1]
        
        # 1. 基础涨幅与相对强度 (RS)
        chg_5d = (curr / close.iloc[-5] - 1)
        chg_20d = (curr / close.iloc[-20] - 1)
        chg_60d = (curr / close.iloc[-60] - 1)
        spy_chg_20 = (spy_df.iloc[-1] / spy_df.iloc[-20] - 1)
        rel_20 = chg_20d - spy_chg_20
        rel_60 = chg_60d - (spy_df.iloc[-1] / spy_df.iloc[-60] - 1)
        
        # 2. 技术指标 (ADR, 量比, 乖离)
        adr_20 = ((high - low) / low).tail(20).mean()
        adr_60 = ((high - low) / low).tail(60).mean()
        ma20 = close.rolling(20).mean().iloc[-1]
        bias_20 = (curr - ma20) / ma20
        avg_vol_20 = vol.tail(20).mean()
        vol_ratio = vol.iloc[-1] / avg_vol_20 if avg_vol_20 > 0 else 0
        
        # 3. IBD RS Raw Score (40/20/20/20 加权)
        rs_raw = ( (curr/close.iloc[-63])*2 + (curr/close.iloc[-126]) + (curr/close.iloc[-189]) + (curr/close.iloc[-252]) )
        
        # 4. 趨勢狀態识别 (VCP + 动量)
        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1]
        rs_line = (close / spy_df).ffill()
        rs_nh = rs_line.iloc[-1] >= rs_line.tail(60).max()
        
        # VCP 紧缩特征：最近波幅显著小于前期
        is_tight = adr_20 < adr_60 * 0.85
        
        action = "观察"
        if rs_nh and curr >= close.tail(126).max() * 0.98: 
            action = "🚀 动量爆发"
        elif is_tight and curr > ma50 and vol_ratio > 1.2:
            action = "🌀 VCP突破"
        elif curr > ma50 > ma200 and rs_nh: 
            action = "💎 核心趋势"
        elif curr > ma20 and low.iloc[-1] < ma20 and (curr - low.iloc[-1])/(high.iloc[-1]-low.iloc[-1]) > 0.6:
            action = "⚔️ 极速反包"

        # 5. 期权异动逻辑 (基于成交量突发和异常波幅)
        options_status = "平稳"
        if vol_ratio > 3.0: options_status = "🔥 机构扫货"
        elif vol_ratio > 2.0 and abs(chg_5d) > 0.08: options_status = "👀 异动预警"
        elif is_tight and vol_ratio > 1.5: options_status = "⚡ 蓄势待发"

        return {
            "Price": curr, "Action": action, "Score": rs_raw, "ADR": adr_20,
            "Vol_Ratio": vol_ratio, "Bias": bias_20, "Options": options_status,
            "Chg_5D": chg_5d, "Chg_20D": chg_20d, "Chg_60D": chg_60d,
            "REL_20": rel_20, "REL_60": rel_60, "RS_Raw": rs_raw
        }
    except: return None

# ==========================================
# 3. 终极视觉输出引擎 (V9.5 - 亮绿看板)
# ==========================================
def final_output(results, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        # --- 顶部状态栏 ---
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header_status = [
            ["🏰 [V750 巅峰 9.5 - 机构级监控看板]", "", "更新时间(北京):", bj_time],
            ["当前天气:", weather, "全美宽度(50MA):", f"{breadth:.1f}%", "VIX恐惧指数:", round(vix, 2)],
            ["策略核心:", "RS强度 + VCP紧缩 + 期权爆发", "状态说明:", "🚀爆发 / 💎核心 / 🌀紧缩 / ⚔️反包"]
        ]
        sh.update(values=header_status, range_name="A1")
        
        if not results: return
        df_final = pd.DataFrame(results)
        
        # --- 字段映射 (按照您的需求排序) ---
        display_cols = [
            "Ticker", "Industry", "Score", "Action", "Resonance", "ADR_20", 
            "Vol_Ratio", "Bias_20", "MktCap(M)", "RS_Rank", "Options", "Price", 
            "5D%", "20D%", "60D%", "REL_20", "REL_60"
        ]
        
        df_show = df_final[display_cols].copy()
        # 数据格式化
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

        # 写入表格 (A5 开始)
        sh.update(values=[df_show.columns.tolist()] + df_show.values.tolist(), range_name="A5")
        
        # --- 🎨 样式渲染 (重点：改成您的亮绿色) ---
        # 1. 亮绿色表头 (对应截图中的颜色)
        sh.format("A5:Q5", {
            "backgroundColor": {"red": 0.45, "green": 0.9, "blue": 0.45}, # 亮绿色
            "textFormat": {"bold": True, "foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        })
        
        # 2. 全局基础排版
        sh.format("A6:Q40", {"horizontalAlignment": "CENTER", "textFormat": {"fontSize": 9}})
        
        # 3. 动态条件格式
        formats = []
        for i, row in df_final.iterrows():
            idx = i + 6
            # 高亮强趋势行
            if "🚀" in row['Action'] or "🌀" in row['Action']:
                formats.append({"range": f"A{idx}:Q{idx}", "format": {"backgroundColor": {"red": 0.9, "green": 1.0, "blue": 0.9}}})
            # 期权异动红色加粗
            if "🔥" in row['Options']:
                formats.append({"range": f"K{idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}}})
            # 行业共振高亮
            if row['Resonance'] >= 3:
                formats.append({"range": f"E{idx}", "format": {"textFormat": {"bold": True, "foregroundColor": {"red": 0.5, "green": 0, "blue": 0.5}}}})

        if formats: sh.batch_format(formats)
        
        # 4. 列宽精细调整
        widths = [60, 130, 60, 100, 75, 75, 65, 65, 90, 65, 100, 75, 65, 65, 65, 65, 65]
        requests = [{"updateDimensionProperties": {"range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}} for i, w in enumerate(widths)]
        client.open_by_key(SHEET_ID).batch_update({"requests": requests})

        print(f"✅ 审计看板刷新成功! 表头已更新为亮绿色。")
    except Exception as e:
        print(f"❌ 输出错误: {e}")

# ==========================================
# 4. 执行流程
# ==========================================
def run_v750_apex_sentinel():
    print("📡 开启全美股扫描引擎...")
    try:
        # 获取标的池
        headers = {'User-Agent': 'Mozilla/5.0'}
        tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        tickers = list(set(tickers + CORE_LEADERS))
        
        # 下载数据 (14个月数据用于计算RS Rank)
        data = yf.download(tickers + ["SPY", "^VIX"], period="14mo", group_by='ticker', threads=True, progress=False)
        spy_df = data["SPY"]["Close"].dropna()
        vix = data["^VIX"]["Close"].iloc[-1]
        
        results_raw = []
        breadth_list = []
        
        for t in tickers:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if len(df_t) < 252: continue
            
            # 市场宽度计算
            if df_t['Close'].iloc[-1] > df_t['Close'].rolling(50).mean().iloc[-1]: breadth_list.append(1)
            
            res = calculate_v750_apex_engine(df_t, spy_df)
            if res and res['Action'] != "观察":
                res['Ticker'] = t
                results_raw.append(res)
        
        if not results_raw: return
        
        # 计算 RS Rank (百分位排序)
        df_all = pd.DataFrame(results_raw)
        df_all['RS_Rank'] = df_all['RS_Raw'].rank(pct=True).apply(lambda x: int(x * 99))
        
        # 筛选最强的 30 只票
        final_candidates = df_all.sort_values("Score", ascending=False).head(30)
        
        # 补充行业和市值 (Info 接口批量调用)
        industry_map = {}
        processed_data = []
        for _, row in final_candidates.iterrows():
            t = row['Ticker']
            try:
                inf = yf.Ticker(t).info
                ind, mkt = inf.get('industry', 'N/A'), inf.get('marketCap', 0) / 1_000_000
                industry_map[ind] = industry_map.get(ind, 0) + 1
            except: ind, mkt = "N/A", 0
            
            d = row.to_dict()
            d.update({"Industry": ind, "MktCap(M)": f"{mkt:,.0f}"})
            processed_data.append(d)
            
        # 填入行业共振数
        for item in processed_data:
            item['Resonance'] = industry_map.get(item['Industry'], 0)
        
        market_breadth = (len(breadth_list) / len(tickers)) * 100
        weather = "☀️" if market_breadth > 60 and vix < 20 else "☁️"
        
        final_output(processed_data, vix, market_breadth, weather)

    except Exception as e:
        print(f"🚨 运行失败: {e}"); traceback.print_exc()

if __name__ == "__main__":
    run_v750_apex_sentinel()
