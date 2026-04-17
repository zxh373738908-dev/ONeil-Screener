import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, warnings, logging, requests, collections
import yfinance as yf

# ================= 配置区 (保持原位) =================
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
TARGET_SHEET_NAME = "A-v7-V53.3-BloodBird"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))
# ====================================================

def init_sheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client.open_by_key(SS_KEY).worksheet(TARGET_SHEET_NAME)

def run_v53_optimizer():
    now = datetime.datetime.now(TZ_SHANGHAI)
    print(f"[{now.strftime('%H:%M:%S')}] 🚀 启动 V53.8 Leader-First (RS强权) 增强版...")

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

    # 2. 获取数据 (多线程批量获取)
    all_data = yf.download(tickers, period="200d", group_by='ticker', progress=True, threads=True)
    
    # 3. RS 评级 (相对强度：权重侧重近期)
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

    # 4. 逻辑扫描 (引入 Rule 3 强权覆盖)
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
            
            this_rs = rs_df[rs_df['code'] == t]['rank'].values[0]
            
            # --- 指标计算 ---
            volat_5 = (c.iloc[-5:].std() / c.iloc[-5:].mean()) * 100
            volat_10 = (c.iloc[-10:].std() / c.iloc[-10:].mean()) * 100
            vcp_signal = "🔥VCP" if volat_5 < volat_10 else "--"
            
            day_high, day_low = df['High'].iloc[-1], df['Low'].iloc[-1]
            close_pos = ((curr_p - day_low) / (day_high - day_low)) * 100 if day_high != day_low else 50
            
            is_vdu = "VDU" if v.iloc[-1] == v.iloc[-10:].min() else "--"
            vol_ratio = v.iloc[-1] / v.iloc[-5:-1].mean()

            # --- 终极打分系统 (加入 Rule 3 加分) ---
            score = this_rs 
            
            # Rule 3: RS 强权覆盖
            if this_rs > 90:
                score += 30 # 给强庄股极大的基础分加成
                status = "👑RS强权(强庄股)"
            else:
                if volat_5 < 3.0 and vol_ratio < 0.9 and close_pos > 60: status = "💎即刻发射"
                else: status = "蓄势中"

            # 技术加分
            if volat_5 < 2.5: score += 15      
            if vcp_signal == "🔥VCP": score += 10 
            if close_pos > 75: score += 10      
            if is_vdu == "VDU": score += 15    
            if ind_map[meta[t]['industry']] >= 5: score += 10 

            # --- 筛选决策层 (Rule 3 重写过滤逻辑) ---
            # 条件 A (Rule 3): RS > 90 且在 MA50 上，统统通过
            # 条件 B (常规): RS > 75 且价格紧致
            is_strong_leader = (this_rs > 90 and curr_p > ma50)
            is_standard_bird = (this_rs > 75 and curr_p > ma50 * 0.97 and volat_5 < 6.0)

            if is_strong_leader or is_standard_bird:
                atr = (df['High'] - df['Low']).rolling(14).mean().iloc[-1]
                stop_l = curr_p - (1.6 * atr)
                target_l = curr_p + (4 * atr)

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

    # 5. 写入与同步
    sh = init_sheet()
    sh.clear()
    if results:
        # 优先级：评分最高者（通常是 RS强权 且 缩量紧致 的品种）
        df_final = pd.DataFrame(results).sort_values("评分", ascending=False).head(40)
        sh.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        print(f"✅ V53.8 Leader-First 完成！Rule 3 覆盖成功。锁定 {len(df_final)} 只标的。")
    else:
        print("⚠️ 未发现标标。")

if __name__ == "__main__":
    run_v53_optimizer()
