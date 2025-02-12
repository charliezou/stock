#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 23:35:14 2025

@author: charlie

增量更新：自动检测本地最新数据日期，仅下载新数据

自动处理复权：使用auto_adjust=True获取调整后价格

实时数据获取：支持分钟级数据（Yahoo Finance实际提供的数据可能有延迟）

数据合并去重：确保数据唯一性

批量操作：支持批量更新股票数据

# 定期更新建议（每日收盘后）
dw.batch_update(["0700", "AAPL"], ["HK", "US"])

# 获取完整历史数据（首次使用时）
dw.update_historical_data("0700", "HK")

# 进行量化分析
data = dw.get_local_data("AAPL", "US")
returns = data['close'].pct_change()

"""

import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta


class YahooFinanceDataWarehouse:
    def __init__(self, root_dir="stock_data"):
        self.root_dir = root_dir
        os.makedirs(root_dir, exist_ok=True)

    def _get_symbol_path(self, symbol, market):
        """获取股票存储路径"""
        market_dir = os.path.join(self.root_dir, market)
        os.makedirs(market_dir, exist_ok=True)
        return os.path.join(market_dir, f"{symbol}.csv")

    def _download_historical_data(self, symbol, start=None, end=None):
        """下载历史数据"""
        try:
            df = yf.download(
                symbol,
                start=start,
                end=end,
                progress=False,
                auto_adjust=True  # 使用调整后的价格
            )
            df = df.rename(columns={
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            return df
        except Exception as e:
            print(f"Error downloading {symbol}: {str(e)}")
            return None

    def update_historical_data(self, symbol, market):
        """更新历史数据（包含初始化下载）"""
        file_path = self._get_symbol_path(symbol, market)
        full_symbol = f"{symbol}.{market}" if market == "HK" else symbol

        # 获取现有数据的最新日期
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path, index_col=0)
            last_date = pd.to_datetime(existing_df.index[-1])
            start_date = last_date + timedelta(days=1)
        else:
            start_date = None  # 全量下载

        # 下载增量数据
        new_data = self._download_historical_data(
            full_symbol,
            start=start_date,
            end=datetime.today().strftime('%Y-%m-%d')
        )

        if new_data is not None and not new_data.empty:
            # 合并数据
            if os.path.exists(file_path):
                updated_df = pd.concat([existing_df, new_data])
            else:
                updated_df = new_data

            # 去重并保存
            updated_df = updated_df[~updated_df.index.duplicated(keep='last')]
            updated_df.to_csv(file_path)
            print(f"Updated {symbol} with {len(new_data)} new records")
        else:
            print(f"No new data for {symbol}")

    def get_realtime_data(self, symbol, market, period="1d"):
        """获取实时数据"""
        full_symbol = f"{symbol}.{market}" if market == "HK" else symbol
        try:
            ticker = yf.Ticker(full_symbol)
            df = ticker.history(
                period=period,
                interval='1m',
                prepost=True  # 包含盘前盘后数据
            )
            return df
        except Exception as e:
            print(f"Error fetching realtime data: {str(e)}")
            return None

    def get_local_data(self, symbol, market):
        """从本地获取数据"""
        file_path = self._get_symbol_path(symbol, market)
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            df.index = pd.to_datetime(df.index)
            return df
        else:
            print(f"No local data found for {symbol} in {market}")
            return None

    def batch_update(self, symbol_list, markets):
        """批量更新股票数据"""
        for symbol, market in zip(symbol_list, markets):
            self.update_historical_data(symbol, market)


# 使用示例
if __name__ == "__main__":
    # 初始化数据仓库
    dw = YahooFinanceDataWarehouse()

    # 股票列表（示例）
    hk_stocks = ["0700", "0005"]  # 港股代码（不带.HK后缀）
    us_stocks = ["AAPL", "MSFT"]  # 美股代码

    # 批量更新港股数据
    dw.batch_update(
        symbol_list=hk_stocks + us_stocks,
        markets=["HK"]*len(hk_stocks) + ["US"]*len(us_stocks)
    )

    # 获取实时数据示例
    realtime_data = dw.get_realtime_data("AAPL", "US")
    print("AAPL最新实时数据：")
    print(realtime_data.tail(2))

    # 从本地读取数据示例
    local_data = dw.get_local_data("0700", "HK")
    print("\n腾讯历史数据概况：")
    print(local_data.describe())
