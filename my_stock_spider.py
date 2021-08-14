import concurrent.futures
import csv
import requests
import random
import json
import re
import numpy as np
import pandas as pd
import os

ENABLE_DOWNLOAD = False

class MyStockSpider(object):

    def __init__(self):

        self.ua_pools = [
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
            'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB7.0)',
            'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; ) AppleWebKit/534.12 (KHTML, like Gecko) Maxthon/3.0 Safari/534.12',
            'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)',
            'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.33 Safari/534.3 SE 2.X MetaSr 1.0',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)',
            'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.41 Safari/535.1 QQBrowser/6.9.11079.201',
            'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E) QQBrowser/6.9.11079.201',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
            'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0'
        ]
        self.host = "https://eniu.com/"
        self.url_all_stocks = "https://eniu.com/static/data/stock_list.json"
        self.url_stock_profit = "https://eniu.com/chart/profita/{0}/q/0/t/table?_=1628905159337"
        self.ua = random.choice(self.ua_pools)
        self.headers = {
            'HOST': self.host,
            'User-Agent': self.ua
        }
        self.stock_list = []

    def get_stock_list(self):
        response = requests.get(self.url_all_stocks)
        if response.status_code == 200:
            response.encoding = 'utf-8'
            self.stock_list = response.json()
            # print(self.stock_list)
        return self.stock_list

    def get_a_stock_revenue_hist(self, a_stock):
        stock_profit_url = self.url_stock_profit.format(a_stock["stock_id"])
        stock_name = a_stock["stock_name"]
        stock_number = a_stock["stock_number"]
        # print(stock_profit_url)
        response = requests.get(stock_profit_url)
        if response.status_code == 200:
            response.encoding = 'gzip'
            stock_profit = response.json()
            # print(stock_profit)
            data_keys = stock_profit[0].keys()
            self.download_data_into_csv("profit/" + stock_number + "_" + stock_name, data_keys, stock_profit)

    def get_all_stock(self, stock_list):
        print("in total of {} stocks".format(len(stock_list)))
        # for a_stock in stock_list:
        #     self.get_a_stock_revenue_hist(a_stock)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(self.get_a_stock_revenue_hist, stock_list)
        print("all downloaded")

    @staticmethod
    def download_data_into_csv(filename, keys, data):
        print("正在下载: {0}".format(filename))
        filepath = 'data/' + filename + '.csv'
        with open(filepath, 'w+', encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
            print("下载完成")


def is_rev_keeps_increasing(stock_file_path, consecutive_count):
    print("checking on {0}".format(stock_file_path))

    df = pd.read_csv(stock_file_path)
    consecutive_count = consecutive_count if df.shape[0] >= consecutive_count else df.shape[0]
    obj = {"stock": stock_file_path, "keeps_increasing": True}
    newer_profit = 0

    for idx, row in df.iterrows():
        # print("index {0}, date {1}, profit {2}".format(idx, row['date'], row['profit']))
        if idx > 0:
            if newer_profit < row['profit']:
                obj["keeps_increasing"] = False
        newer_profit = row['profit']
        consecutive_count -= 1
        if consecutive_count == 0 or not obj["keeps_increasing"]:
            return obj

    return obj


def scan_files_for_criteria(func, consecutive_count, list_file_paths):
    wanted_stocks = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_stock_selected = {executor.submit(func, fpath, consecutive_count): fpath for fpath in list_file_paths}
        for future in concurrent.futures.as_completed(future_stock_selected):
            # is_selected_obj = future_stock_selected[future]
            # try:
            is_selected_obj = future.result()
            if is_selected_obj["keeps_increasing"]:
                wanted_stocks.append(is_selected_obj)
            # if is_selected_obj["keeps_increasing"]:
            #     print("%s keeps increasing for %s years!" % (is_selected_obj["stock"], consecutive_count))
            # except Exception as exc:
            #     print('%r generated an exception: %s' % (is_selected_obj, exc))
    print("Done with scanning, in total of %s stocks are selected" % len(wanted_stocks))
    return wanted_stocks


if __name__ == '__main__':
    # download raw data
    if ENABLE_DOWNLOAD:
        ss = MyStockSpider()
        all_stocks = ss.get_stock_list()
        to_download = [stock for stock in all_stocks if (stock["stock_pinyin"][0:2] != "ST"
                                                         and len(stock["stock_number"]) == 6)]
        ss.get_all_stock(to_download)

    # analyze data
    files_to_read = []
    for (root, dirs, files) in os.walk('data/profit/'):
        for file in files:
            # if file == "600645_中源协和.csv":
            file_path = os.path.join(root, file)
            files_to_read.append(file_path)

    result_stocks = scan_files_for_criteria(is_rev_keeps_increasing, 5, files_to_read)
    for idx, val in enumerate(result_stocks):
        print("%d - wanted: %s" % (idx, val))
    # increasing = is_rev_keeps_increasing('data/profit/600645_中源协和.csv', 3)
    # print(increasing)


