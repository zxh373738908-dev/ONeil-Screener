import akshare as ak
import pandas as pd
import numpy as np
import datetime
import concurrent.futures
import gspread
from google.oauth2.service_account import Credentials
import time
import random
import warnings
import traceback

warnings.filterwarnings('ignore')

# ==========================================
# Google Sheet 配置
# ==========================================
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

def get_worksheet():
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    try:
        return doc.worksheet("A-Share Screener")
    except gspread.exceptions.WorksheetNotFound:
        print("   -> ⚠️ 未找到 'A-Share Screener' 工作表，系统正在自动创建...")
        return doc.add_worksheet(title="A-Share Screener", rows=100, cols=20)

# ==========================================
# 🛡️ 核心防封锁重试装饰器/机制
# ==========================================
def robust_fetch(fetch_func, retries=3, delay=2):
    """带指数退避的防封获取器"""
    for attempt in range(retries):
        try:
            return fetch_func()
        except Exception as e:
            sleep_time = delay + random.uniform(1, 3) * attempt
            print(f"      [警告] 接口波动 ({str(e)[:30]})... {sleep_time:.1f}秒后重试 ({attempt+1}/{retries})")
            time.sleep(sleep_time)
    return None

# ==========================================
# STEP1: 东方财富主线雷达 (带防封优化)
# ==========================================
def get_hot_tickers():
    print("\n🔥 STEP1 启动【东方财富】主线雷达，扫描今日最强资金风口...")
    try:
        # 使用 robust_fetch 防止第一次请求就被阻断
        ind_df = robust_fetch(lambda: ak.stock_board_industry_name_em(), retries=3)
        if ind_df is None or ind_df.empty:
            raise ValueError("获取板块数据为空")

        top_inds = ind_df.sort_values(by="涨跌幅", ascending=False).head(5)
        hot_sectors = top_inds["板块名称"].tolist()
        print(f"   -> ✅ 锁定当前市场前 5 大强势行业：{hot_sectors}")
        
        hot_tickers = set()
        for sector in hot_sectors:
            try:
                # 增加随机停顿，避免触发反爬
                time.sleep(random.uniform(0.8, 1.5))
                cons_df = ak.stock_board_industry_cons_em(symbol=sector)
                codes = [str(x).zfill(6) for x in cons_df["代码"].tolist()]
                hot_tickers.update(codes)
            except Exception as e:
                print(f"   -> ⚠️ 板块 [{sector}] 成分股获取波动: {str(e)[:30]}")
                
        if not hot_tickers:
            raise ValueError("成分股解析为空")
            
        print(f"   -> 🎯 主线雷达扫描完毕，共提取 {len(hot_tickers)} 只主线热门标的！")
        return list(hot_tickers)
    except Exception as e:
        print(f"   -> ❌ 主线雷达受阻 ({str(e)[:40]})，系统将自动降级为全市场盲扫模式。")
        return []

# ==========================================
# STEP2: 市场快照与基底过滤 (多数据源备用)
# ==========================================
def get_market_snapshot(hot_tickers):
    print("\n🚀 STEP2 抓取全市场快照，并进行兵力筛选...")
    
    df = None
    # 方案A: 东方财富 (数据最全)
    try:
        df = robust_fetch(lambda: ak.stock_zh_a_spot_em(), retries=2)
        if df is not None and not df.empty:
            df = df.rename(columns={"代码": "code", "名称": "name", "最新价": "price", "总市值": "mktcap"})
            print("   -> ✅ 成功获取东方财富全市场快照。")
    except:
        pass
        
    # 方案B: 如果东财被封，启用新浪备用节点
    if df is None or df.empty:
        print("   -> ⚠️ 东财节点失效，正在切换至【新浪备用节点】...")
        try:
            df = robust_fetch(lambda: ak.stock_zh_a_spot_sina(), retries=2)
            if df is not None and not df.empty:
                df = df.rename(columns={"symbol": "code", "name": "name", "trade": "price", "mktcap": "mktcap"})
                # 新浪代码带 sh/sz 前缀，需清洗
                df["code"] = df["code"].str[-6:]
                print("   -> ✅ 成功获取新浪全市场快照。")
        except:
            pass
            
    if df is None or df.empty:
        print("   -> ❌ 致命错误：所有数据源节点均已失效！")
        return pd.DataFrame()

    try:
        # 保障字段存在
        df = df[["code", "name", "price", "mktcap"]].dropna()
        df["price"] = pd.to_numeric(df["price"], errors='coerce')
        df["mktcap"] = pd.to_numeric(df["mktcap"], errors='coerce')
        
        # 基础过滤：过滤低价仙股(>5元)和极小盘(>40亿)
        df = df[(df["price"] > 5) & (df["mktcap"] > 4000000000)]
        
        if hot_tickers:
            df = df[df["code"].isin(hot_tickers)]
            print(f"   -> ⚔️ 剔除非主线后，剩余 {len(df)} 只【主线精锐】等待 K 线演算。")
        else:
            # ⚠️【关键防封】：如果是盲扫，只取市值最大的前 300 只，避免几千次K线请求直接封死 Github IP
            df = df.sort_values(by="mktcap", ascending=False).head(300)
            print(f"   -> ⚔️ 盲扫模式启动，为防 IP 封禁，已截取头部 {len(df)} 只核心大盘标的等待演算。")
            
        return df
    except Exception as e:
        print(f"   -> ❌ 数据清洗失败: {e}")
        return pd.DataFrame()

# ==========================================
# STEP3: K线获取 (内置防爆盾)
# ==========================================
def get_kline(code):
    def _fetch():
        df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        if df is None or df.empty: 
            return None
        # 🛡️ 盘前防误杀：如果是今日未开盘的 0 成交量，切掉最后一行
        if df.iloc[-1]["成交量"] == 0:
            df = df.iloc[:-1]
        if len(df) < 200: # 稍微降低要求到 200 天
            return None
        return df

    # 单只股票内部也加入防封错机制
    for attempt in range(2):
        try:
            return _fetch()
        except Exception:
            time.sleep(1 + random.random() * 2)
    return None

# ==========================================
# STEP4: RS与RSI计算引擎 (Pandas 向量化加速)
# ==========================================
def calc_rs(close_array):
    if len(close_array) < 121: return 0
    c_current = close_array[-1]
    r20 = (c_current - close_array[-21]) / close_array[-21]
    r60 = (c_current - close_array[-61]) / close_array[-61]
    r120 = (c_current - close_array[-121]) / close_array[-121]
    return r20 * 0.4 + r60 * 0.3 + r120 * 0.3

def calc_rsi(prices_series, periods=14):
    delta = prices_series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=periods).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=periods).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50

# ==========================================
# STEP5: 三大游资战法判定
# ==========================================
def analyze_stock(row):
    try:
        # 添加随机微小停顿，打散线程池并发请求时间点，防被侦测
        time.sleep(random.uniform(0.1, 0.5))
        
        code = row["code"]
        name = row["name"]

        df = get_kline(code)
        if df is None: return None

        close = df["收盘"].values
        volume = df["成交量"].values
        
        if len(close) < 200: return None

        ma20 = close[-20:].mean()
        ma50 = close[-50:].mean()
        ma150 = close[-150:].mean()
        ma200 = close[-200:].mean()
        price = close[-1]

        rs = calc_rs(close)
        # 转换为 Series 以利用 Pandas 向量化 RSI
        rsi = calc_rsi(pd.Series(close[-30:]))
        
        vol_mean_50 = volume[-50:].mean()
        vol_ratio = volume[-1] / vol_mean_50 if vol_mean_50 > 0 else 1

        # -------------------------
        # 1. 右侧突破 (起飞阶段)
        # -------------------------
        breakout = (
            price > ma20 and price > ma50
            and ma50 > ma150 > ma200
            and vol_ratio > 1.5
            and rsi > 60
        )

        # -------------------------
        # 2. 左侧伏击 (缩量踩生命线)
        # -------------------------
        ambush = (
            abs(price - ma20) / ma20 < 0.03
            and vol_ratio < 1.1
            and ma50 > ma150 > ma200
        )

        # -------------------------
        # 3. 黄金坑 (鲁西化工模式：老龙回头)
        # -------------------------
        high60 = close[-60:].max()
        pit = (
            price < high60 * 0.85
            and price >= ma50 * 0.98  
            and vol_ratio > 1.3
        )

        if not (breakout or ambush or pit):
            return None

        if pit: type_label = "🐉 黄金坑"
        elif breakout: type_label = "🔥 突破起飞"
        else: type_label = "🧘 缩量伏击"

        return {
            "Ticker": code,
            "Name": name,
            "Price": round(price, 2),
            "RS_Score": round(rs * 100, 2),
            "RSI": round(rsi, 2),
            "Vol_Ratio": round(vol_ratio, 2),
            "Type": type_label
        }
    except Exception:
        return None

# ==========================================
# STEP6: 主扫描调度器 (安全并发版)
# ==========================================
def scan_market():
    hot_tickers = get_hot_tickers()
    spot = get_market_snapshot(hot_tickers)
    
    if spot.empty: 
        print("❌ 兵力筛选失败，无法获取大盘数据。")
        return []

    print(f"\n   -> ⚙️ 正在向数据中心请求 K 线演算 (启用降速防封策略)...")
    results = []
    
    # ⚠️【关键】：Github Actions 环境下将并发降低至 3，宁可慢一分钟，绝不报错中断！
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as exe:
        futures = {exe.submit(analyze_stock, row): row for _, row in spot.iterrows()}
        completed = 0
        total = len(futures)
        
        for f in concurrent.futures.as_completed(futures):
            completed += 1
            if completed % 50 == 0:
                print(f"      ... 已扫描 {completed} / {total} 只标的")
                
            res = f.result()
            if res: results.append(res)

    return results

# ==========================================
# STEP7: 写入作战指令
# ==========================================
def write_sheet(data):
    print("\n📝 STEP3 正在将绝密作战名单写入 Google Sheets 表格...")
    try:
        sheet = get_worksheet()
        sheet.clear()

        if len(data) == 0:
            sheet.update_acell("A1", "No Signal: 当前战局恶劣或遭遇防爬阻击，未发现符合狙击条件的标的。")
            print("⚠️ 筛选完毕，已写入空仓报告！")
            return

        df = pd.DataFrame(data)
        # 按照 RS 评分从高到低排序，呈现真正的龙头
        df = df.sort_values("RS_Score", ascending=False)

        sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
        
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sheet.update_acell("H1", "Last Update:")
        sheet.update_acell("I1", now)
        
        print(f"🎉 大功告成！已成功将 {len(df)} 只【主线 + 三大战法】双重认证龙头送达指挥部！")
    except Exception as e:
        print(f"❌ 表格写入致命失败: {e}")
        traceback.print_exc()

# ==========================================
# MAIN
# ==========================================
def main():
    print("\n========== A股猎手系统 V6.2 (防封锁重装版) ==========")
    data = scan_market()
    write_sheet(data)

if __name__ == "__main__":
    main()
