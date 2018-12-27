import xlrd
import requests
import os
from datasource import utils
import aiohttp
import asyncio
from datetime import timedelta,date,datetime


class SHExchange:
    __headers = {
        'Referer': 'http://www.sse.com.cn/assortment/stock/list/share/'
        }
    __base_url = "http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=%d"
    
    @classmethod
    def __get(cls, url):
        res = requests.get(url, headers=cls.__headers,timeout=2.5)
        res.raise_for_status()
        return res.text

    @classmethod
    def get_trading_codes(cls):
        text = cls.__get(cls.__base_url.replace('%d', '1'))
        h = utils.CSVHelper(text)
        next(h)
        return (row[0:6] for row in h)

    @classmethod
    def get_delisted_codes(cls):
        text = cls.__get(cls.__base_url.replace('%d', '5'))
        h = utils.CSVHelper(text)
        next(h)
        return (row[0:6] for row in h if not row.startswith('900'))
    
    @classmethod
    def get_halted_codes(cls):
        text = cls.__get(cls.__base_url.replace('%d', '4'))
        h = utils.CSVHelper(text)
        next(h)
        return (row[0:6] for row in h if not row.startswith('900'))
    
    
class SZExchange:
    __base_url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=%s&TABKEY=tab%d'
    
    @classmethod
    def __get(cls,url,col=0):
        res = requests.get(url,timout=10)
        res.raise_for_status()
        with open('tmp.xlsx', 'wb') as f:
            f.write(res.content)
        wb = xlrd.open_workbook('tmp.xlsx')
        sheet = wb.sheet_by_index(0)
        ret = (sheet.row(i)[col].value 
               for i in range(1, sheet.nrows) 
               if not sheet.row(i)[col].value.startswith('200'))
        os.remove('tmp.xlsx')
        return ret
    
    @classmethod
    def get_trading_codes(cls):
        url = cls.__base_url % ('1110', 1)
        return cls.__get(url)
    
    @classmethod
    def get_delisted_codes(cls):
        url = cls.__base_url % ('1793_ssgs', 2)
        return cls.__get(url)
    
    @classmethod
    def get_halted_codes(cls):
        url = cls.__base_url % ('1793_ssgs', 1)
        return cls.__get(url)

    
class Wangyi:
    __open_date = '19901219'
    __stocks_folder = 'E:/quant/data/wangyi/stocks/'
    __session = None
    
    @classmethod
    def make_stock_url(cls, code, end_date, start_date):
        url =  "http://quotes.money.163.com/service/chddata.html?code={0}&start={1}&end={2}&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP".format(
            utils.add_wangyi_prefix(code),
            start_date, 
            end_date)
        return url
    
    @classmethod
    def get_stock_day_data(cls, code, end_date, start_date=__open_date):
        url = cls.make_stock_url(code,end_date,start_date)
        response = requests.get(url,timeout=5)
        response.raise_for_status()
        return response.text
    
    @classmethod
    async def aget_stock_day_data(cls, code, end_date, start_date=__open_date):
        url = cls.make_stock_url(code, end_date, start_date)
        if cls.__session is None:
            cls.__session = aiohttp.ClientSession()
        async with cls.__session.get(url) as response:
            return await response.text()
    
    @classmethod
    async def acomplement(cls, file_name, end_date):
        last_date = datetime.strptime(file_name[0:10], '%Y-%m-%d').date()
        code = file_name[11:17]
        if end_date > last_date:
            if last_date.weekday() == 4:
                start_date = last_date+timedelta(3)
            else:
                start_date = last_date+timedelta(1)
            content = await cls.aget_stock_day_data(code, end_date.strftime('%Y%m%d'), start_date.strftime('%Y%m%d'))
            helper = utils.CSVHelper(content)
            next(helper)
            try:
                latest_date = next(helper)[0:10]
                path = cls.__stocks_folder + file_name
                with open(path, 'a') as f:
                    f.writelines(utils.WYRCSVHelper(content,62))
                os.rename(path, cls.__stocks_folder + latest_date + '-' + '%s.csv' % code)
            except StopIteration:
                return

    @classmethod
    async def acomplemt_all(cls):
        end_date = date.today()
        if end_date.weekday() == 0:
            end_date = end_date-timedelta(3)
        await asyncio.gather(*[asyncio.create_task(Wangyi.acomplement(file_name, end_date)) for file_name in os.listdir(cls.__stocks_folder)])

    @classmethod
    def complement_all(cls):
        asyncio.run(cls.acomplemt_all())


Wangyi.complement_all()
