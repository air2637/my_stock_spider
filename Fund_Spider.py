import requests
import random
import json
import re
import threading
import numpy as np
import pandas as pd
import os
from lxml import etree
from openpyxl import load_workbook

class FundSpider(object):
    def __init__(self):
        self.uapools = [
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
        self.ua = random.choice(self.uapools)
        # self.sem = threading.Semaphore(100)
        self.stock_dict = dict() # ??????????????????


    # ???xml?????????json
    # def xmltojson(self,xmlstr):
    #     xmlparse = xmltodict.parse(xmlstr)
    #     jsonstr = json.dumps(xmlparse,indent=1)
    #     return jsonstr

    # ??????????????????
    @staticmethod
    def getFunName(code):
        url = "http://fund.eastmoney.com/pingzhongdata/{}.js".format(code)
        response = requests.get(url)
        name = re.search(r'.*fS_name = "(.*?)";var',response.text).group(1)
        return name

    # ????????????????????????
    def getFundData(self,code_list,stime,etime):
        pd.DataFrame().to_excel('result_fund.xlsx')  # excel??????????????????????????????sheet??????????????????????????????sheet
        self.hosts = "fund.eastmoney.com"
        self.headers = {
            'HOST': self.hosts,
            'User-Agent': self.ua
        }
        print("???????????????????????????...")
        for code in code_list:
            self.data_list = []
            self.url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={0}&sdate={1}&edate={2}&per=20&page=1'.format(code,stime,etime) # ????????????????????????
            response = requests.get(self.url,headers = self.headers)
            html = re.search(r'.*content:"(.*?)",records:.*pages:(.*?),curpage*',response.text)
            pages = html.group(2) # ????????????
            print("????????????{0}?????????{1}???...".format(code, pages))
            for page in range(int(pages)):
                self.url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={0}&sdate={1}&edate={2}&per=20&page={3}'.format(
                    code, stime, etime,page+1)
                response = requests.get(self.url, headers=self.headers)
                if response.status_code == 200:
                    response.encoding = 'utf-8'
                    html = re.search(r'.*content:"(.*?)",records:.*pages:(.*?),curpage*', response.text)
                    content_xml = html.group(1)  # ????????????
                    content = etree.HTML(content_xml).xpath('//tr/td') # ?????????xml
                    for info in content:
                        self.data_list.append(info.text)
            # ???List?????????pandas??????
            data_np = np.array(self.data_list).reshape(-1,7)
            data_df = pd.DataFrame(data_np,columns=['????????????','????????????','????????????','????????????','????????????','????????????','????????????'])
            name = self.getFunName(code) + '(' + code + ')'
            self.downLoadData(name,data_df)

    # ?????????????????????
    def downLoadData(self,name,data):
        writer = pd.ExcelWriter('result_fund.xlsx')
        self.excelAddSheet(data, writer, name)

    # ????????????????????????sheet?????????
    @staticmethod
    def excelAddSheet(dataframe, excelWriter, sheet_name):
        book = load_workbook(excelWriter.path)
        excelWriter.book = book
        dataframe.to_excel(excel_writer=excelWriter, sheet_name=sheet_name, index=None)
        excelWriter.close()
        print("{0}??????????????????!!!".format(sheet_name))

    # ????????????sheet?????????csv
    @staticmethod
    def mergeData(code_list):
        data = pd.read_excel('result_fund.xlsx',None)
        data_sheet = list(data.keys())
        print(data_sheet)
        df = pd.DataFrame(columns=['????????????','????????????','????????????','????????????','????????????','????????????','????????????','????????????'])
        for j,i in enumerate(data_sheet):
            data[i]['????????????'] = code_list[j]
            df_temp = data[i]
            df = pd.concat([df,df_temp])
        df.to_excel('result_fund_sum.xlsx',index=None)

if __name__ == '__main__':
    code_list = ['005911','006476','006649','110022','006479','005918','320007','320010','501010','001593','001156','007301',
                 '160225','161028','360016','762001','519778','002939','006113','003095','004041','001551','003884',
                 '100038','001549','007874','006098','377240','004070','001178','005689','050026','161005','001508',
                 '001510','161017']
    fund = FundSpider()
    # fund.getFundData(code_list,'2019-01-01','2020-02-07')
    fund.mergeData(code_list)