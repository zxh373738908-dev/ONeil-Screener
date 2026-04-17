import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests, collections
import yfinance as yf

# ================= 配置区 =================
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))
# ==========================================

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client.open_by_key(SS_KEY).worksheet(TARGET_SHEET_NAME)

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 启动 V53.7 Quantum-Bird 终极增强版...")

    # 1. 扫描大池子
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "industry", "close", "market_cap_basic"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 85e8}], 
        "range": [0, 800]
    }
    raw_data = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
    
    tickers, meta, ind_map = [], {}, collections.Counter()
    for item in raw_data:
        code = item['s'].split(':')[-1]
        yf_code = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        tickers.append(yf_code)
        industry = item['d'][2] or "Others"
        meta[yf_code] = {"name": item['d'][1], "industry": industry, "symbol": code}
        ind_map[industry] += 1

    # 2. 获取数据
    all_data = yf.download(tickers, period="200d", group_by='ticker', progress=True, threads=True)
    
    # 3. RS 评级 (相对强度)
    rs_list = []
    for t in tickers:
        try:
            c = all_data[t]['Close'].dropna()
            if len(c) < 120: continue
            score = (c.iloc[-1]/c.iloc[-20]*0.5) + (c.iloc[-1]/c.iloc[-60]*0.3) + (c.iloc[-1]/c.iloc[-120]*0.2)
            rs_list.append({"code": t, "score": score})
        except: continue
    rs_df = pd.DataFrame(rs_list)
    rs_df['rank'] = rs_df['score'].rank(pct=True) * 99

    # 4. 量价微观扫描
    results = []
    for t in tickers:
        try:
            df = all_data[t].dropna()
            if len(df) < 50: continue
            
            c = df['Close']
            v = df['Volume']
            curr_p = float(c.iloc[-1])
            ma20 = c.rolling(20).mean().iloc[-1]
            ma50 = c.rolling(50).mean().iloc[-1]
            
            # --- 优化指标计算 ---
            this_rs = rs_df[rs_df['code'] == t]['rank'].values[0]
            
            # (1) VCP 强度：5日波动 vs 10日波动
            volat_5 = (c.iloc[-5:].std() / c.iloc[-5:].mean()) * 100
            volat_10 = (c.iloc[-10:].std() / c.iloc[-10:].mean()) * 100
            vcp_signal = "🔥VCP" if volat_5 < volat_10 else "--"
            
            # (2) 收盘价位置：今天是否收在全天高位 (0-100)
            day_high, day_low = df['High'].iloc[-1], df['Low'].iloc[-1]
            close_pos = ((curr_p - day_low) / (day_high - day_low)) * 100 if day_high != day_low else 50
            
            # (3) VDU 绝对枯竭检测
            is_vdu = "VDU" if v.iloc[-1] == v.iloc[-10:].min() else "--"
            vol_ratio = v.iloc[-1] / v.iloc[-5:-1].mean()

            # --- 终极打分系统 ---
            score = this_rs 
            if volat_5 < 2.5: score += 15      # 极度紧致加分
            if vcp_signal == "🔥VCP": score += 10 # 波动收敛加分
            if close_pos > 70: score += 10      # 强力收盘加分
            if is_vdu == "VDU": score += 15    # 绝对地量加分
            if ind_map[meta[t]['industry']] >= 5: score += 10 # 行业共振

            # 状态判定
            status = "等待突破"
            if volat_5 < 3.0 and vol_ratio < 0.9 and close_pos > 60: status = "💎即刻发射"
            elif this_rs > 95: status = "👑最强领头羊"

            # 筛选准则
            if this_rs > 75 and curr_p > ma50 * 0.97:
                results.append({
                    "评分": int(score),
                    "RS评级": int(this_rs),
                    "代码": meta[t]['symbol'],
                    "名称": meta[t]['name'],
                    "现价": round(curr_p, 2),
                    "量比": round(vol_ratio, 2),
                    "紧致度": f"{round(volat_5, 2)}%",
                    "50日乖离": f"{round(((curr_p-ma50)/ma50)*100, 1)}%",
                    "状态": status,
                    "VCP/VDU": f"{vcp_signal}/{is_vdu}",
                    "行业": meta[t]['industry'],
                    "更新": now.strftime('%H:%M')
                })
        except: continue

    # 5. 写入
    sh = init_sheet()
    sh.clear()
    if results:
        df_final = pd.DataFrame(results).sort_values(["评分", "RS评级"], ascending=[False, False]).head(35)
        # 调整列顺序，把最关键的放前面
        sh.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        print(f"✅ V53.7 Quantum-Bird 完成！已锁定 {len(df_final)} 只极品标的。")
    else:
        print("⚠️ 未发现标的。")

if __name__ == "__main__":
    run_v53_optimizer()
