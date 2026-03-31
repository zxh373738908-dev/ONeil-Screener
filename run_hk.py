import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import math
import time

warnings.filterwarnings('ignore')

# ==========================================
# 1. 策略配置中心
# ==========================================
# 扩充票池：恒指蓝筹 + 恒生科技 + 重点红利股
HK_UNIVERSE = [
    "0700.HK", "3690.HK", "9988.HK", "1211.HK", "1810.HK", "9888.HK", "0941.HK", 
    "2318.HK", "0005.HK", "0388.HK", "1024.HK", "9618.HK", "2015.HK", "2269.HK", 
    "1177.HK", "0857.HK", "0883.HK", "0386.HK", "1398.HK", "0939.HK", "3988.HK",
    "2628.HK", "2319.HK", "0992.HK", "2020.HK", "2331.HK", "1088.HK", "1880.HK"
]

SHEET_ID = "您的_GOOGLE_SHEET_ID"
creds_file = "credentials.json"
ACCOUNT_SIZE = 200000  # 建议 20 万港币基准
MAX_RISK_PER_TRADE = 0.01 # 单笔损失控制在总仓位 1%

# ==========================================
# 🛡️ 核心计算引擎
# ==========================================
def calculate_advanced_v750_hk(df, hsi_df, tech_df):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        turnover = close * vol # 日成交额
        
        # 1. 趋势过滤 (Minervini Stage 2)
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        is_stage_2 = close.iloc[-1] > ma50.iloc[-1] > ma150.iloc[-1] and ma200.rolling(20).mean().iloc[-1] > ma200.iloc[-20]

        # 2. RS 评分 (Mansfield 改进版: (P/Bench) / SMA(P/Bench, 50) - 1)
        rs_ratio = close / hsi_df
        mansfield_rs = (rs_ratio.iloc[-1] / rs_ratio.tail(50).mean()) - 1
        rs_nh = rs_ratio.iloc[-1] >= rs_ratio.tail(252).max()

        # 3. 机构脚印 (成交额异动)
        avg_turnover = turnover.tail(20).mean()
        if avg_turnover < 80000000: return None # 必须日均 > 8000万港币
        vol_surge = turnover.iloc[-1] / avg_turnover # 当日成交额对比 20 日均值

        # 4. VCP 紧致度判定
        tightness_10d = (close.tail(10).std() / close.tail(10).mean()) * 100
        
        # 5. 综合动作决策
        action = "观察"
        # 信号 A：奇点先行 (RS新高 + 价格横盘)
        if rs_nh and close.iloc[-1] < close.tail(20).max() * 1.02:
            action = "👁️ 奇点(RS Stealth)"
        # 信号 B：机构抢筹 (放量突破)
        elif is_stage_2 and vol_surge > 1.5 and close.iloc[-1] > ma50.iloc[-1]:
            action = "🚀 抢筹(Institutional)"
        # 信号 C：VCP 极致收缩
        elif is_stage_2 and tightness_10d < 1.1:
            action = "💎 紧致(VCP)"

        # 6. 风控计算 (考虑港股每手不确定性，返回建议股数)
        adr_20 = ((high - low)/low).tail(20).mean()
        stop_loss_pct = max(adr_20 * 1.5, 0.05) # 至少 5% 空间
        stop_price = close.iloc[-1] * (1 - stop_loss_pct)
        
        risk_per_share = close.iloc[-1] - stop_price
        shares_to_buy = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) / risk_per_share if risk_per_share > 0 else 0

        return {
            "action": action, "rs_score": round(mansfield_rs, 3), "price": close.iloc[-1],
            "stop": round(stop_price, 2), "shares": int(shares_to_buy), 
            "vol_surge": round(vol_surge, 2), "tight": round(tightness_10d, 2),
            "is_stage_2": is_stage_2, "rs_nh": rs_nh, "turnover_m": round(avg_turnover/1000000, 1)
        }
    except Exception as e:
        return None

# ==========================================
# 3. 执行主逻辑
# ==========================================
def run_hk_v750_pro():
    print(f"🏮 [V750 HK Pro] 正在启动全港股动量探测...")
    
    # 1. 获取市场基准 (恒指, 恒生科技)
    benchmarks = yf.download(["^HSI", "^HSTECH"], period="2y", progress=False)['Close']
    hsi_df = benchmarks["^HSI"].dropna()
    tech_df = benchmarks["^HSTECH"].dropna()
    
    # 2. 批量获取数据
    all_tickers = list(set(HK_UNIVERSE))
    data = yf.download(all_tickers, period="2y", group_by='ticker', threads=True, progress=False)
    
    results = []
    print(f"🔎 正在审计 {len(all_tickers)} 只核心港股...")
    
    for t in all_tickers:
        if t not in data.columns.levels[0]: continue
        df = data[t].dropna()
        if len(df) < 250: continue
        
        analysis = calculate_advanced_v750_hk(df, hsi_df, tech_df)
        
        if analysis and analysis['action'] != "观察" and analysis['is_stage_2']:
            # 港股特色评级：RS新高 + 放量 = 钻石级
            tier = "💎SSS" if (analysis['rs_nh'] and analysis['vol_surge'] > 1.2) else "🔥强势"
            
            results.append({
                "代码": t, "评级": tier, "信号": analysis['action'],
                "RS得分": analysis['rs_score'], "现价": analysis['price'],
                "建议股数": analysis['shares'], "止损参考": analysis['stop'],
                "成交放大": analysis['vol_surge'], "紧致度%": analysis['tight'],
                "日均成交(M)": analysis['turnover_m'], "RS新高": "★" if analysis['rs_nh'] else ""
            })

    # 3. 输出至 Google Sheets
    export_to_sheets(results, hsi_df.iloc[-1])

def export_to_sheets(res_list, hsi_last):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("HK_V750_PRO")
        sh.clear()
        
        # 头部信息
        status = "🐂 牛市多头" if hsi_last > 19000 else "🐻 熊市震荡" # 简单阈值
        header = [
            ["🏰 V750 港股巅峰扫描仪 (机构增强版)", "", "执行时间:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["恒指状态:", round(hsi_last, 0), "风控建议:", "单笔风险 1% / 港股严禁左侧抄底"],
            ["指标说明:", "成交放大 > 1.2 表示机构扫货; 紧致度 < 1.1 表示洗盘完成。"],
            []
        ]
        sh.update(values=header, range_name="A1")
        
        if res_list:
            df_final = pd.DataFrame(res_list).sort_values(by="RS得分", ascending=False)
            # 数据清洗
            data_to_write = [df_final.columns.tolist()] + df_final.values.tolist()
            # 这里的每一项都需要清理
            clean_matrix = [[str(cell) if isinstance(cell, (datetime.date, datetime.datetime)) else cell for cell in row] for row in data_to_write]
            sh.update(values=clean_matrix, range_name="A5")
            print(f"✅ 成功下达指令：捕捉到 {len(res_list)} 个大师级形态。")
        else:
            sh.update_acell("A5", "📭 当前全市场未发现符合 Stage 2 & VCP 的优质信号。")
            
    except Exception as e:
        print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_hk_v750_pro()
