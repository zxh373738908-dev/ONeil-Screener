import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. Google Sheets 连接
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"  
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔突破 + 老龙回头 双核扫描器
# ==========================================
def get_us_tickers():
    print("\n========== [1/3] 开始获取美股核心目标群 ==========")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        sp_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        sp500 = next(df['Symbol'].tolist() for df in sp_tables if 'Symbol' in df.columns)
        
        ndx_tables = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)
        ndx100 = next(df['Ticker'].tolist() for df in ndx_tables if 'Ticker' in df.columns)
        
        tickers = list(set([str(t).replace('.', '-') for t in (sp500 + ndx100)]))
        print(f"✅ 成功获取并去重，共计 {len(tickers)} 只核心股票。")
        return tickers
    except Exception as e:
        print(f"❌ 获取美股列表失败: {e}")
        return []

def screen_us_stocks():
    tickers = get_us_tickers()
    if not tickers: return []
    
    print("\n========== [2/3] 启动华尔街并发下载引擎 (极速版) ==========")
    # 下载历史数据用于技术面分析
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
    print("✅ 历史数据下载完成，进入量化雷达扫描...")

    tech_passed_stocks = []
    
    for ticker in tickers:
        try:
            if ticker not in data.columns.levels[0]:
                continue
                
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            close = float(df['Close'].iloc[-1])
            volume = float(df['Volume'].iloc[-1])
            
            # 基础流动性过滤：股价 < 15 或 基础日成交额 < 5000万美金 剔除
            if close < 15 or (close * volume) < 50000000: continue
            
            # --- 极速算法：用 tail().mean() 替代滚动计算，速度提升数十倍 ---
            avg_vol_50 = df['Volume'].tail(50).mean()
            vol_ratio = volume / avg_vol_50 if avg_vol_50 > 0 else 0
            
            ma20 = df['Close'].tail(20).mean()
            ma50 = df['Close'].tail(50).mean()
            ma150 = df['Close'].tail(150).mean()
            ma200 = df['Close'].tail(200).mean()
            high_250 = df['High'].tail(250).max()
            
            # 距离 52周新高的回撤幅度 (负数)
            dist_high = (close - high_250) / high_250
            
            # 收益率计算 (证明是老龙)
            ret_90d = (close - float(df['Close'].iloc[-63])) / float(df['Close'].iloc[-63])
            ret_120d = (close - float(df['Close'].iloc[-126])) / float(df['Close'].iloc[-126]) if len(df) >= 126 else ret_90d
            
            # RSI 极速计算 (仅取最后14天计算，节省算力)
            delta = df['Close'].tail(30).diff()
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            ema_up = up.ewm(com=13, adjust=False).mean()
            ema_down = down.ewm(com=13, adjust=False).mean()
            rs = ema_up / ema_down
            rsi = float(100 - (100 / (1 + rs)).iloc[-1])

            # ====================================================
            # ⚔️ 战术 1：欧奈尔主升突破 (Breakout) - 蓝天无阻力区
            # ====================================================
            is_breakout = (
                (ma50 > ma200) and 
                (close > ma150 and close > ma200) and
                (dist_high >= -0.15) and       # 突破形态：距离最高点不超 15%
                (ret_90d >= 0.15) and          # 3个月涨幅 > 15%
                (rsi >= 55) and                # 强势区
                (close > ma20)                 # 站在 20 日线之上 (强势)
            )

            # ====================================================
            # 🧬 战术 2：老龙回头专属核心逻辑 (Pullback to 50-Day Line)
            # ====================================================
            is_pullback = (
                (ret_90d >= 0.15 or ret_120d >= 0.15) and # 核心1：前期涨幅超15%，证明是真老龙
                (-0.20 <= dist_high <= -0.10) and         # 核心2：洗盘距高点回撤 10%-20%，完美黄金坑
                (ma50 * 0.97 <= close <= ma50 * 1.05) and # 核心3：股价靠近50日线（允许下探诱空3%或悬空5%）
                (close < ma20)                            # 核心4：被压在20日线下方，散户绝望阶段
            )

            # 如果均不满足，直接过滤
            if not (is_breakout or is_pullback): continue

            # 结构标签识别
            if is_pullback and is_breakout:
                struct_label = "🔥 终极双核"
            elif is_pullback:
                struct_label = "🐉 老龙回头 (靠近50日线)"
            else:
                struct_label = "🚀 强势突破"

            # 暂存通过技术面筛选的标的
            tech_passed_stocks.append({
                "Ticker": ticker,
                "Price": round(close, 2),
                "90D_Return%": round(ret_90d * 100, 2), # 留作数字方便排序
                "120D_Return%": round(ret_120d * 100, 2),
                "RSI": round(rsi, 2),
                "Vol_Ratio": round(vol_ratio, 2),
                "Dist_High%": f"{round(dist_high * 100, 2)}%",
                "Turnover(M)": round((close * volume) / 1000000, 2),
                "Struct": struct_label
            })
            
        except Exception as e:
            continue
            
    print(f"⚡ 技术面初筛完成，共选出 {len(tech_passed_stocks)} 只标的。")
    print("========== 正在进行基本面深度核查 (市值 > 20亿) ==========")
    
    final_stocks = []
    # 巧妙逻辑：只对通过技术面的十几只股票发起网络请求查询市值，避免500次请求导致死机
    for stock in tech_passed_stocks:
        try:
            ticker_obj = yf.Ticker(stock["Ticker"])
            # 获取市值，如果没有数据则默认为0
            market_cap = ticker_obj.info.get('marketCap', 0)
            market_cap_b = market_cap / 1_000_000_000 # 转换为 Billions (十亿)
            
            # 核心过滤：市值必须大于 20亿 (2 Billion)
            if market_cap_b >= 2.0:
                stock["Market_Cap(B)"] = round(market_cap_b, 2)
                # 格式化返回值
                stock["90D_Return%"] = f"{stock['90D_Return%']}%"
                stock["120D_Return%"] = f"{stock['120D_Return%']}%"
                final_stocks.append(stock)
                print(f"🎯 战术锁定: {stock['Ticker']} [{stock['Struct']}] | 市值: {round(market_cap_b,1)}B")
            else:
                print(f"🗑️ 剔除 {stock['Ticker']}：市值仅 {round(market_cap_b, 2)}B，不足20亿！")
        except:
            continue
            
    return final_stocks

def write_to_sheet(sheet_name, final_stocks):
    print("\n========== [3/3] 开始同步至指挥中心 (Google Sheets) ==========")
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            # 排序逻辑：优先按结构分组，然后按量比和90天涨幅排序
            df['Sort_Num'] = df['90D_Return%'].str.replace('%', '').astype(float)
            df = df.sort_values(by=['Struct', 'Sort_Num'], ascending=[False, False]).drop(columns=['Sort_Num'])
            
            # 调整列顺序，把市值放在前面
            cols = ["Ticker", "Struct", "Market_Cap(B)", "Price", "90D_Return%", "120D_Return%", "Dist_High%", "Vol_Ratio", "RSI", "Turnover(M)"]
            df = df[cols]
            
            data_to_write = [df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("L1", "Last Updated:")
            sheet.update_acell("M1", now_time)
            print(f"🎉 成功！{len(df)} 只最强龙头已装填至 {sheet_name}！")
        else:
            sheet.clear()
            msg = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 当下无符合条件的股票，大盘极度恶劣，防守为主。"
            sheet.update_acell("A1", msg)
            print(f"⚠️ {sheet_name}: 已输出空仓警告。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

if __name__ == "__main__":
    us_results = screen_us_stocks()
    write_to_sheet("Screener", us_results)
