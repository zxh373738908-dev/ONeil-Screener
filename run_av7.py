import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests
import yfinance as yf

# ================= 配置区 =================
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

# 策略阈值：根据当前高波动市场做了自适应调整
MIN_RS_RANK = 70       # RS 评级前 30%
MAX_BIAS_20 = 12.0     # 允许距离 20 日线稍远（因为强势股拉升快）
# ==========================================

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client.open_by_key(SS_KEY).worksheet(TARGET_SHEET_NAME)

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 启动 V53.5 自适应早鸟扫描器...")

    # 1. 获取标的池 (增加量能过滤，确保是活跃股)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "industry", "close", "market_cap_basic", "volume"],
        "filter": [
            {"left": "market_cap_basic", "operation": "greater", "right": 80e8},
            {"left": "volume", "operation": "greater", "right": 1000000} # 过滤僵尸股
        ], 
        "range": [0, 800]
    }
    raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    
    tickers, meta = [], {}
    for item in raw_data:
        code = item['s'].split(':')[-1]
        yf_code = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        tickers.append(yf_code)
        meta[yf_code] = {"name": item['d'][1], "industry": item['d'][2], "symbol": code}

    # 2. 获取数据
    print(f"📥 正在分析 {len(tickers)} 只标的...")
    all_data = yf.download(tickers, period="150d", group_by='ticker', progress=True, threads=True)
    
    # 3. 计算 RS 评级 (相对强度)
    rs_results = []
    for t in tickers:
        try:
            c = all_data[t]['Close'].dropna()
            if len(c) < 60: continue
            # 改进版 RS：更侧重近期表现（1个月权重加大）
            score = (c.iloc[-1]/c.iloc[-20]*0.5) + (c.iloc[-1]/c.iloc[-60]*0.3) + (c.iloc[-1]/c.iloc[-120]*0.2)
            rs_results.append({"code": t, "score": score})
        except: continue
    
    rs_df = pd.DataFrame(rs_results)
    rs_df['rank'] = rs_df['score'].rank(pct=True) * 99

    # 4. 扫描并打分
    final_results = []
    for t in tickers:
        try:
            df = all_data[t].dropna()
            if len(df) < 30: continue
            
            close = df['Close']
            curr_p = float(close.iloc[-1])
            ma20 = close.rolling(20).mean().iloc[-1]
            ma50 = close.rolling(50).mean().iloc[-1]
            
            # --- 指标计算 ---
            this_rs = rs_df[rs_df['code'] == t]['rank'].values[0]
            # 紧致度 (取5日波幅)
            tightness = (close.iloc[-5:].std() / close.iloc[-5:].mean()) * 100
            # 量比 (今日对比5日均量)
            vol_ratio = df['Volume'].iloc[-1] / df['Volume'].iloc[-5:-1].mean()
            # 距离均线
            bias_20 = ((curr_p - ma20) / ma20) * 100
            
            # --- 核心打分模型 (寻找爆发潜质) ---
            # 基础分：RS 评级
            score = this_rs 
            # 加分项：如果量比在 0.6-1.2 之间（缩量蓄势），加 10 分
            if 0.5 < vol_ratio < 1.3: score += 10
            # 加分项：如果紧致度 < 4%，加 10 分
            if tightness < 4.0: score += 10
            # 减分项：如果 bias_20 > 15%（涨太猛了），减 20 分
            if bias_20 > 15: score -= 20
            
            # 止损与目标
            atr = (df['High'] - df['Low']).rolling(14).mean().iloc[-1]
            stop_l = curr_p - (1.6 * atr)
            target_l = curr_p + (4 * atr)

            # --- 过滤：只保留 RS 评级较高且趋势不坏的 ---
            if this_rs > MIN_RS_RANK and curr_p > ma50:
                final_results.append({
                    "评分": int(score),
                    "RS评级": int(this_rs),
                    "代码": meta[t]['symbol'],
                    "名称": meta[t]['name'],
                    "现价": round(curr_p, 2),
                    "量比": round(vol_ratio, 2),
                    "紧致度": f"{round(tightness, 2)}%",
                    "50日乖离": f"{round(((curr_p-ma50)/ma50)*100, 1)}%",
                    "盈亏比": round((target_l - curr_p)/(curr_p - stop_l), 2),
                    "行业": meta[t]['industry'],
                    "止损": round(stop_l, 2),
                    "目标": round(target_l, 2),
                    "更新": now.strftime('%H:%M')
                })
        except: continue

    # 5. 写入 Google Sheets (保证有结果)
    sh = init_sheet()
    sh.clear()
    
    if final_results:
        # 按总分排序（总分结合了 RS 和蓄势形态）
        df_final = pd.DataFrame(final_results).sort_values("评分", ascending=False).head(25)
        sh.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        print(f"✅ 扫描结束！成功锁定 {len(df_final)} 只候选，已按爆发潜力排序。")
    else:
        sh.update_acell("A1", f"最后扫描: {now.strftime('%H:%M')} - 市场极度异常，无合适标的")
        print("⚠️ 仍无结果，请检查 yfinance 网络连接。")

if __name__ == "__main__":
    run_v53_optimizer()
