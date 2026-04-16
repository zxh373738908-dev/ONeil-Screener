import pandas as pd
import numpy as np
import requests
import re
import yfinance as yf
import datetime
import logging

# ==========================================
# 0. 系统配置与日志初始化
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
pd.options.mode.chained_assignment = None  # 忽略部分 Pandas 警告

# ==========================================
# 1. 核心量化算法 (原 calculate_advanced_v750 的实现)
# ==========================================
def calculate_advanced_v750(df, hsi_series):
    """
    量化核心逻辑：判断主升浪、相对强度(RS)与成交量异动
    返回字典格式结果或 None
    """
    try:
        if len(df) < 150: return None # 剔除次新股
        
        # 计算技术指标
        close = df['Close']
        vol = df['Volume']
        
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        vol_ma50 = vol.rolling(50).mean()
        
        current_price = close.iloc[-1]
        current_vol = vol.iloc[-1]
        
        # 1. 趋势多头排列 (Minervini 模板核心)
        trend_bull = (current_price > ma20.iloc[-1]) and \
                     (ma20.iloc[-1] > ma50.iloc[-1]) and \
                     (ma50.iloc[-1] > ma150.iloc[-1])
                     
        # 2. 成交量异动 (今日量 > 50日均量的 1.2倍)
        vol_ratio = current_vol / vol_ma50.iloc[-1] if vol_ma50.iloc[-1] > 0 else 0
        
        # 3. 相对大盘强度 (RS)
        # 个股近60日涨幅 / 恒指近60日涨幅
        stock_ret = current_price / close.iloc[-60] - 1
        hsi_ret = hsi_series.iloc[-1] / hsi_series.iloc[-60] - 1
        rs_raw = stock_ret - hsi_ret
        
        # 综合打分 (动能 + 趋势)
        score = (vol_ratio * 10) + (rs_raw * 100)
        
        # 判断 Action
        action = "观察"
        if trend_bull and vol_ratio > 1.2 and rs_raw > 0:
            action = "🚀主升浪" if vol_ratio > 2.0 else "👁️奇点突破"
            
        return {
            "is_bull": trend_bull,
            "Action": action,
            "Score": score,
            "rs_raw": rs_raw,
            "Price": round(current_price, 2),
            "Vol_Ratio": round(vol_ratio, 2),
            "RS_Vel": round(rs_raw, 3),
            "Stop": round(current_price * 0.92, 2), # 默认8%止损
            "Shares": int(100000 / current_price),  # 假设10w资金单笔建议股数
            "Tight": "Yes" if close.iloc[-10:].std() / close.iloc[-10:].mean() < 0.05 else "No"
        }
    except Exception as e:
        return None

# ==========================================
# 2. 策略执行主类
# ==========================================
class QuantumScanner:
    def __init__(self):
        self.hsi_p = 0
        self.hsi_ma50 = 0
        self.hsi_series = None
        self.weather = "❄️ 观望"
        self.df_pool = pd.DataFrame()
        
    def fetch_benchmark(self):
        logging.info("1. 正在获取大盘基准...")
        hsi_raw = yf.download("^HSI", period="300d", progress=False)['Close']
        # 兼容 Pandas 多种返回结构
        self.hsi_series = hsi_raw.iloc[:,0] if isinstance(hsi_raw, pd.DataFrame) else hsi_raw
        self.hsi_p = self.hsi_series.iloc[-1]
        self.hsi_ma50 = self.hsi_series.rolling(50).mean().iloc[-1]
        self.weather = "☀️ 激进" if self.hsi_p > self.hsi_ma50 else "❄️ 观望"
        logging.info(f"大盘环境: {self.weather} (收盘:{self.hsi_p:.0f}, MA50:{self.hsi_ma50:.0f})")

    def scan_tradingview(self):
        logging.info("2. 正在扫描 TradingView 全市场票池...")
        url = "https://scanner.tradingview.com/hongkong/scan"
        payload = {
            "columns":["name", "description", "close", "market_cap_basic", "sector"],
            "filter":[{"left": "market_cap_basic", "operation": "greater", "right": 8e9}], # 80亿以上
            "range": [0, 1000], 
            "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
        }
        try:
            resp = requests.post(url, json=payload, timeout=15).json().get('data', [])
            self.df_pool = pd.DataFrame([
                {"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "其他"} 
                for d in resp
            ])
            logging.info(f"TV粗筛完成，获取到 {len(self.df_pool)} 只基础标的。")
        except Exception as e:
            logging.error(f"TradingView 获取失败: {e}")
            raise

    def process_and_rank(self):
        logging.info("3. 正在拉取 Yahoo Finance 详情并执行量化测算...")
        final_list = []
        tickers =[str(c).zfill(4) + ".HK" for c in self.df_pool['code']]
        
        # 批量下载数据
        data = yf.download(tickers, period="1y", group_by='ticker', progress=False, threads=True, auto_adjust=False)
        
        # 获取含有数据的股票列名
        available_tickers = data.columns.levels[0] if isinstance(data.columns, pd.MultiIndex) else[]
        
        for t in tickers:
            try:
                if t not in available_tickers: continue
                
                # 清洗停牌或缺失数据
                stock_data = data[t].dropna()
                if len(stock_data) < 100: continue 
                
                res = calculate_advanced_v750(stock_data, self.hsi_series)
                
                if res and res.get('is_bull') and res.get('Action') != "观察":
                    code_raw = t.split('.')[0].lstrip('0')
                    sector = self.df_pool[self.df_pool['code']==code_raw].iloc[0]['sector']
                    res.update({"Ticker": t.split('.')[0], "Sector": sector})
                    final_list.append(res)
            except Exception as e:
                # 屏蔽单一股票的错误，防止打断整个循环
                continue

        if not final_list:
            logging.warning("当前无股票满足突破/主升浪条件。")
            return pd.DataFrame()

        res_df = pd.DataFrame(final_list)
        
        # 4. 排序与动态配额
        logging.info("4. 正在执行动态配额与板块评级...")
        res_df['Final_Score'] = res_df['Score'] + res_df['rs_raw'].rank(pct=True)*25
        
        # 多头市场每个板块允许 8 只，空头市场只允许 3 只
        sector_limit = 8 if self.weather == "☀️ 激进" else 3 
        
        top_picks = res_df.sort_values(by="Final_Score", ascending=False).groupby('Sector').head(sector_limit)
        return top_picks.head(60)

    def export_results(self, df):
        if df.empty: return
        
        logging.info("5. 结果汇总:")
        cols =["Ticker", "Action", "Final_Score", "Price", "Shares", "Stop", "Tight", "Vol_Ratio", "RS_Vel", "Sector"]
        output_df = df[cols]
        
        # 在终端打印美化过的表格
        print("\n" + "="*80)
        print(f"🏰 V45-V750 量子领袖版 | 环境: {self.weather} | 时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("="*80)
        print(output_df.to_string(index=False))
        print("="*80)
        
        # 如果你有 Google Sheets 配置，可以在这里接入 GSpread 代码
        # df.to_csv("Quantum_Picks.csv", index=False) # 本地备份

# ==========================================
# 3. 运行入口
# ==========================================
if __name__ == "__main__":
    scanner = QuantumScanner()
    try:
        scanner.fetch_benchmark()
        scanner.scan_tradingview()
        results = scanner.process_and_rank()
        scanner.export_results(results)
        logging.info("✅ 任务流完全结束。")
    except Exception as e:
        logging.error(f"❌ 程序运行阻断: {e}")
