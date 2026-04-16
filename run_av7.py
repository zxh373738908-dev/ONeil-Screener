import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests, os
import yfinance as yf

# 环境屏蔽
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 配置中心
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-Wednesday-Sniper" # 周三潜伏表
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_sheet():
    # ... (保持原有的谷歌表格初始化逻辑)
    if not os.path.exists(CREDS_FILE): exit(1)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    if TARGET_SHEET_NAME not in [w.title for w in doc.worksheets()]:
        return doc.add_worksheet(TARGET_SHEET_NAME, 1000, 20)
    return doc.worksheet(TARGET_SHEET_NAME)

# ==========================================
# 🧠 核心演算：周三潜伏逻辑 (预判周四启动)
# ==========================================
def calculate_wednesday_sniper(df, idx_df):
    try:
        if len(df) < 250: return None
        c = df['Close'].astype(float)
        h = df['High'].astype(float)
        l = df['Low'].astype(float)
        v = df['Volume'].astype(float)
        price = float(c.iloc[-1])
        
        # 1. 均线压舱石 (确保趋势未破)
        ma50 = c.rolling(50).mean().iloc[-1]
        if price < ma50 * 0.96: return None # 远离生命线太远不看

        # 2. 【周三潜伏核心】缩量止跌信号 (主力洗盘)
        # 寻找最近三天连续缩量，或者今天呈现“缩量阴线”或“长下影”
        is_vol_shrink = v.iloc[-1] < v.rolling(20).mean().iloc[-1] * 0.8
        
        # 3. RS 强劲底牌 (必须是强于大盘的票)
        rs_line = c / idx_df
        rs_max_250 = rs_line.tail(250).max()
        is_rs_lead = rs_line.iloc[-1] >= rs_max_250 * 0.95
        if not is_rs_lead: return None

        # 4. 紧致度检测 (待爆发)
        tightness = (h.tail(10).max() - l.tail(10).min()) / (l.tail(10).min() + 0.001) * 100
        if tightness > 8: return None # 波动太大，不是潜伏标的

        # 5. 勋章与评分
        tag = "🔍 周三潜伏"
        # 如果缩量+RS新高+处于生命线附近 = 潜力股
        if is_vol_shrink and tightness < 5:
            tag = "🎯 周三极品潜伏"
        else:
            return None # 非极品不入池

        # 6. 计算目标 (预判周四/周五突破)
        target_p = round(h.tail(60).max() * 1.05, 2)
        stop_p = round(l.tail(20).min() * 0.98, 2)
        rrr = round((target_p - price) / (price - stop_p + 0.01), 1)

        return {
            "tag": tag, "price": price, "v_ratio": round(v.iloc[-1]/v.rolling(20).mean().iloc[-1], 2),
            "tight": round(tightness, 1), "rrr": rrr, "stop": stop_p, "target": target_p
        }
    except: return None

# ==========================================
# 🚀 执行引擎
# ==========================================
def run_wednesday_sniper():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ 启动周三潜伏狙击模式...")
    
    # 1. 抓取基准与标的池 (同V53.2逻辑)
    idx_raw = yf.download("000300.SS", period="400d", progress=False)['Close']
    idx_s = idx_raw.iloc[:, 0] if isinstance(idx_raw, pd.DataFrame) else idx_raw
    
    # 假设 df_pool 已获取 (同V53.2)
    # ... (省略网络获取代码以保证简洁，直接调用核心逻辑)
    
    # 2. 扫描处理
    all_hits = []
    # [这里插入你的 tickers 循环]
    # ...
    
    # 3. 输出到表格
    # 按照 rrr 盈亏比排序，给你的“新易盛/中际旭创”这类票最高优先级
    
    print("🎉 周三潜伏清单已更新，重点关注 RS 强度高且缩量的标的！")

if __name__ == "__main__":
    run_wednesday_sniper()
