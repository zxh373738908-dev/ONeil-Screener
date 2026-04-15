import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import warnings
import math

warnings.filterwarnings('ignore')

# ==========================================
# 1. 策略配置中心
# ==========================================
# 請確保這是你最新部署的 GAS Webhook URL
WEBAPP_URL = "https://script.google.com/macros/s/AKfycby1pIM7iO43lcLQpOmi5LCJIn3VN9a0Ilf9amoy1EtQV_GBXJkk_A4PpsrJxKzH7i51/exec"
SHEET_TAB_NAME = "🚀右側_動能成長"

# 排除的板塊/行業
EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'Credit Services']

# ==========================================
# 2. 大盤環境濾網 (Market Regime Filter)
# ==========================================
def check_market_trend():
    """檢查標普500 (SPY) 是否處於多頭趨勢 (價格 > 50日均線)"""
    try:
        spy = yf.download("SPY", period="6mo", progress=False)['Close']
        if isinstance(spy, pd.DataFrame): spy = spy.iloc[:, 0]
        
        curr_spy = float(spy.iloc[-1])
        ma50_spy = float(spy.tail(50).mean())
        
        is_bull_market = curr_spy > ma50_spy
        return is_bull_market, curr_spy, ma50_spy
    except Exception as e:
        print("⚠️ 無法獲取大盤數據，默認放行。")
        return True, 0, 0

# ==========================================
# 3. 獲取股票池 (S&P 500 作為高流動性代表)
# ==========================================
def get_universe():
    print("📡 正在獲取基礎股票池 (大型股)...")
    try:
        tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        df = tables[0]
        tickers = [t.replace('.', '-') for t in df['Symbol'].tolist()]
        return tickers
    except:
        return ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO", "CRWD", "PLTR"]

# ==========================================
# 4. 智能基本面評估 (Z-Score + Rule of 40)
# ==========================================
def calculate_fundamentals(ticker):
    """
    動態評估：科技股使用 Rule of 40，傳統行業使用 Altman Z-Score
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        sector = info.get('sector', '')
        industry = info.get('industry', '')
        
        # 1. 剔除金融地雷
        for ex in EXCLUDED_INDUSTRIES:
            if ex.lower() in sector.lower() or ex.lower() in industry.lower():
                return False, 0, "金融/保險股剔除", info
                
        # 2. 基礎規模要求 (營收 > 10億)
        rev = info.get('totalRevenue') or 0
        if rev < 1_000_000_000:
            return False, 0, "營收規模不足", info

        # 3. 科技股專屬：矽谷 40 法則 (Rule of 40)
        is_tech = 'Technology' in sector or 'Software' in industry
        
        if is_tech:
            fcf = info.get('freeCashflow') or 0
            rev_growth = info.get('revenueGrowth') or 0
            fcf_margin = fcf / rev if rev > 0 else 0
            
            rule_40_score = (rev_growth + fcf_margin) * 100
            
            if rule_40_score >= 40: # 滿足40法則即為極品科技股
                return True, rule_40_score, f"Rule of 40: {rule_40_score:.1f}%", info
            else:
                return False, 0, "未達科技股40法則", info
                
        # 4. 傳統行業：Altman Z-Score 簡化版
        else:
            bs = stock.balance_sheet
            inc = stock.financials
            if bs.empty or inc.empty: return False, 0, "無財務報表", info
            
            ta = bs.loc['Total Assets'].iloc[0] if 'Total Assets' in bs.index else 1
            tl = bs.loc['Total Liabilities Net Minority Interest'].iloc[0] if 'Total Liabilities Net Minority Interest' in bs.index else 1
            ebit = inc.loc['EBIT'].iloc[0] if 'EBIT' in inc.index else 0
            mkt_cap = info.get('marketCap', 1)
            
            # 簡化算法，抓取核心健康度
            x3 = ebit / ta      # 資產回報
            x4 = mkt_cap / tl   # 債務健康度
            
            z_score = (3.3 * x3) + (0.6 * x4)
            if z_score > 2.0:
                return True, z_score * 10, f"Z-Score 安全 ({z_score:.2f})", info
            else:
                return False, 0, "Z-Score 偏低", info

    except Exception as e:
        return False, 0, "數據抓取失敗", {}

# ==========================================
# 5. 主指揮引擎
# ==========================================
def run_growth_momentum_scanner():
    start_time = time.time()
    print("🚀 [CAN SLIM 動能成長策略] 啟動...")
    
    # 【優化1】大盤環境審查
    is_bull, curr_spy, ma50_spy = check_market_trend()
    if not is_bull:
        print(f"🛑 大盤轉弱 (SPY {curr_spy:.2f} 跌破 50MA {ma50_spy:.2f})，停止右側建倉。")
        sync_to_google_sheet(is_bull=False)
        return

    print("✅ 大盤處於多頭趨勢，允許右側交易。")
    tickers = get_universe()

    try:
        data = yf.download(tickers, period="1y", interval="1d", group_by='ticker', auto_adjust=True, threads=True, progress=False)
    except Exception as e:
        print(f"❌ 數據下載失敗: {e}"); return

    tech_candidates = []

    # 第一層：技術面漏斗初篩
    for t in tickers:
        try:
            df = data[t].dropna() if isinstance(data.columns, pd.MultiIndex) else data.dropna()
            if len(df) < 200: continue
            
            close, vol = df['Close'], df['Volume']
            curr_price = float(close.iloc[-1])
            
            # 流動性與均線多頭
            vol_10d = vol.tail(10).mean()
            if vol_10d < 500_000: continue
            
            ma20 = close.tail(20).mean()
            ma50 = close.tail(50).mean()
            ma200 = close.tail(200).mean()
            if not (curr_price > ma20 and ma20 > ma50 and ma50 > ma200): continue
            
            # RS 動能計算
            ret_1m = (curr_price - close.iloc[-21]) / close.iloc[-21] if len(close) >= 21 else 0
            ret_3m = (curr_price - close.iloc[-63]) / close.iloc[-63] if len(close) >= 63 else 0
            ret_1y = (curr_price - close.iloc[-252]) / close.iloc[-252] if len(close) >= 252 else 0
            
            rs_score = (ret_1m * 0.4) + (ret_3m * 0.3) + (ret_1y * 0.3)
            if rs_score < 0: continue
            
            tech_candidates.append({
                "Ticker": t, "Price": curr_price, "RS_Score": rs_score,
                "Ret_1M": ret_1m, "Ret_3M": ret_3m
            })
        except: continue

    # 取 RS 最強的前 40 隻進入基本面體檢
    tech_candidates = sorted(tech_candidates, key=lambda x: x['RS_Score'], reverse=True)[:40]
    print(f"⚔️ 技術面初篩完成，選出 {len(tech_candidates)} 隻強勢股進入基本面體檢...")

    final_candidates = []
    
    # 第二層：基本面深查
    for cand in tech_candidates:
        t = cand['Ticker']
        passed, fin_score, fin_msg, info = calculate_fundamentals(t)
        
        if passed:
            total_score = cand['RS_Score'] * 100 + fin_score
            final_candidates.append({
                "Ticker": t, "Price": cand['Price'], "Sector": info.get('sector', 'Unknown'),
                "RS_Score": cand['RS_Score'] * 100, "Fin_Score": fin_score,
                "Total_Score": total_score, "Details": fin_msg,
                "1M_Ret": cand['Ret_1M'] * 100, "3M_Ret": cand['Ret_3M'] * 100
            })

    top_10 = sorted(final_candidates, key=lambda x: x['Total_Score'], reverse=True)[:10]
    sync_to_google_sheet(is_bull=True, data_list=top_10, exec_time=time.time() - start_time)

# ==========================================
# 6. 格式化與雲端同步 (支援多工作表)
# ==========================================
def sync_to_google_sheet(is_bull, data_list=None, exec_time=0):
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    
    header = [
        ["🚀 動能成長 Top 10 (右側策略)", "更新:", bj_now, "大盤狀態:", "🟢 多頭允許交易" if is_bull else "🔴 轉弱暫停交易", "", "", "", "", ""],
        ["排名", "代碼", "板塊", "現價", "近1月漲幅", "近3月漲幅", "RS動能分", "基本面護城河", "綜合總分", "交易紀律"]
    ]
    
    final_list = []
    if not is_bull:
        final_list.append(["-", "大盤跌破50MA", "系統判定為危險期", "-", "-", "-", "-", "保留現金", "-", "觀望 / 嚴禁右側追高"])
    elif not data_list:
        final_list.append(["-", "無符合標的", "-", "-", "-", "-", "-", "-", "-", "-"])
    else:
        for i, row in enumerate(data_list):
            final_list.append([
                f"Top {i+1}", str(row['Ticker']), str(row['Sector']), round(float(row['Price']), 2),
                f"{round(float(row['1M_Ret']), 1)}%", f"{round(float(row['3M_Ret']), 1)}%",
                round(float(row['RS_Score']), 2), str(row['Details']), round(float(row['Total_Score']), 2),
                "回踩 20EMA 買入 / 破 50MA 止損" # 【優化3】明確進出場點
            ])

    matrix = header + final_list
    
    try:
        def safe_json_val(val):
            if isinstance(val, float) and not math.isfinite(val): return 0
            return str(val)
            
        matrix_clean = json.loads(json.dumps(matrix, default=safe_json_val))
        
        # 【核心變更】使用新的 payload 結構，指定分頁名稱
        payload = {
            "sheet_name": SHEET_TAB_NAME, 
            "data": matrix_clean
        }
        
        requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 雲端同步完成！發送至分頁: [{SHEET_TAB_NAME}] | 耗時: {round(exec_time, 2)}秒")
    except Exception as e:
        print(f"❌ 雲端同步失敗: {e}")

if __name__ == "__main__":
    run_growth_momentum_scanner()
