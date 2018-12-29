import xlrd
import requests
import os
from datasource import utils
import aiohttp
import asyncio
from datetime import timedelta, datetime
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd


class SHExchange:
    __headers = {
        'Referer': 'http://www.sse.com.cn/assortment/stock/list/share/'
        }
    __base_url = "http://query.sse.com.cn/security/stock/downloadStockListFile.do?" \
                 "csrcCode=&stockCode=&areaName=&stockType=%d"
    
    @classmethod
    def __get(cls, url):
        res = requests.get(url, headers=cls.__headers, timeout=2.5)
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

    @classmethod
    def get_all_codes(cls):
        for g in cls.get_trading_codes(), cls.get_delisted_codes(), cls.get_halted_codes():
            yield from g


class SZExchange:
    __base_url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=%s&TABKEY=tab%d'
    
    @classmethod
    def __get(cls, url, col=0):
        res = requests.get(url, timeout=10)
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

    @classmethod
    def get_all_codes(cls):
        for g in cls.get_trading_codes(), cls.get_delisted_codes(), cls.get_halted_codes():
            yield from g


def get_all_codes():
    for g in SHExchange.get_all_codes(), SZExchange.get_all_codes():
        yield from g


class Wangyi:
    __open_date = '19901219'
    __stocks_folder = 'E:/quant/data/wangyi/stocks/'

    @classmethod
    def make_stock_url(cls, code, end_date, start_date):
        url = "http://quotes.money.163.com/service/chddata.html?" \
               "code={0}&start={1}&end={2}&" \
              "fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP".format(
                utils.add_wangyi_prefix(code),
                start_date,
                end_date)
        return url

    @classmethod
    def make_end_date(cls):
        now = datetime.now()
        today = now.date()
        stamp = datetime(today.year, today.month, today.day, 19)
        if now > stamp:
            return today
        else:
            return today - timedelta(1)

    @classmethod
    def get_stock_day_data(cls, code, end_date, start_date=__open_date):
        url = cls.make_stock_url(code, end_date, start_date)
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.text
    
    @classmethod
    async def aget_stock_day_data(cls, code, end_date, start_date=__open_date):
        url = cls.make_stock_url(code, end_date, start_date)
        async with cls.__session.get(url) as response:
            return await response.text(encoding='gbk')
    
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
                    f.writelines(utils.WYRCSVHelper(content, 62))
                os.rename(path, cls.__stocks_folder + latest_date + '-' + '%s.csv' % code)
            except StopIteration:
                return

    @classmethod
    async def acomplemt_all(cls):
        end_date = cls.make_end_date()
        if end_date.weekday() == 0:
            end_date = end_date-timedelta(3)
        sh_codes = set(SHExchange.get_trading_codes())
        sz_codes = set(SZExchange.get_trading_codes())

        def _filter(file_name):
            code = file_name[11:17]
            return code in sh_codes or code in sz_codes
        
        async with aiohttp.ClientSession() as client:
            cls.__session = client
            await asyncio.gather(*tuple(asyncio.create_task(Wangyi.acomplement(file_name, end_date))
                                        for file_name in os.listdir(cls.__stocks_folder) if _filter(file_name)))
        del cls.__session

    @classmethod
    async def adownload(cls, code, end_date):
        content = await cls.aget_stock_day_data(code, end_date)
        helper = utils.CSVHelper(content)
        next(helper)
        try:
            latest_date = next(helper)[0:10]
        except StopIteration:
            print('there is no data for %s' % code)
            return
        with open(cls.__stocks_folder + latest_date + '-' + '%s.csv' % code, 'w', newline='') as f:
            f.write(content[0:62])
            helper = utils.WYRCSVHelper(content, 62)
            f.writelines(helper)

    @classmethod
    async def adownload_all(cls, skip_existed=True):
        end_date = cls.make_end_date()
        codes = get_all_codes()
        if skip_existed:
            downloaded_codes = set(name[11:17] for name in os.listdir(cls.__stocks_folder))
        async with aiohttp.ClientSession() as client:
            cls.__session = client
            await asyncio.gather(*tuple(cls.adownload(code, end_date) for code in codes
                                        if not skip_existed or code not in downloaded_codes))
        del cls.__session

    @classmethod
    async def apeek(cls, code, s_date):
        url = "http://quotes.money.163.com/trade/lsjysj_%s.html#01b07" % code
        async with cls.__session.get(url) as response:
            content = await response.text(encoding='utf-8')
        soup = BeautifulSoup(content, 'html.parser')
        table = soup.findAll('table')[3]
        ret = []
        for tr in table.findAll('tr'):
            data = [td.text for td in tr.findAll('td')]
            if not data:
                continue
            if data[0] <= s_date:
                return ret
            else:
                data[7] = data[7].replace(',', '')
                data[8] = data[8].replace(',', '')
                ret.append(data)
        return ret

    @classmethod
    async def apeek_complement(cls, df):
        r = df.iloc[len(df)-1]
        code = r['股票代码']
#       注意如果缺的数据有除权那么市值和流通市值是不准的
        shares1 = r['总市值'] / r['收盘价']
        shares2 = r['流通市值'] / r['收盘价']
        datas = await cls.apeek(code[1:], r.name.date().strftime('%Y-%m-%d'))
        for data in reversed(datas):
            row = {'股票代码': code, '名称': r['名称'], '收盘价': np.float(data[4]),
                   '最高价': np.float(data[2]), '开盘价': np.float(data[1]), '最低价': np.float(data[3]), '涨跌额': np.float(data[5]),
                   '涨跌幅': np.float(data[6]), '成交量': int(data[7])*100, '成交金额': np.float(data[8])*10000, '换手率': np.float(data[10]),
                   '前收盘': r['收盘价'], '总市值': shares1*np.float(data[4]), '流通市值': shares2*np.float(data[4])}
            _date = pd.Timestamp.strptime(data[0], '%Y-%m-%d')
            df.loc[_date] = row

    @classmethod
    async def apeek_complement_all(cls, dfs):
        end_date = cls.make_end_date()
        async with aiohttp.ClientSession() as client:
            cls.__session = client
            await asyncio.gather(*tuple(cls.apeek_complement(df) for df in dfs if df.index[-1].date() < end_date))
        del cls.__session

    @classmethod
    def read_all_stocks(cls):
        return (pd.read_csv(cls.__stocks_folder + name, encoding='gbk', index_col=0, converters={
                                '日期': lambda s_date: datetime.strptime(s_date, '%Y-%m-%d'),
                                '收盘价': utils.NoneZeroFloat,
                                '最高价': utils.NoneZeroFloat,
                                '最低价': utils.NoneZeroFloat,
                                '开盘价': utils.NoneZeroFloat,
                                '前收盘': utils.NoneZeroFloat,
                                '涨跌额': utils.Float,
                                '涨跌幅': utils.Float,
                                '换手率': utils.NoneZeroFloat,
                                '成交量': utils.NoneZeroInt,
                                '成交金额': utils.NoneZeroFloat,
                                '总市值': utils.NoneZeroFloat,
                                '流通市值': utils.NoneZeroFloat
                                }) for name in os.listdir(cls.__stocks_folder))

    @classmethod
    def read_stock(cls, code):
        names = (name for name in os.listdir(cls.__stocks_folder))
        for name in names:
            if code in name:
                return pd.read_csv(cls.__stocks_folder+name, encoding='gbk', index_col=0,
                                   converters={
                                    '日期': lambda s_date: datetime.strptime(s_date, '%Y-%m-%d'),
                                    '收盘价': utils.NoneZeroFloat,
                                    '最高价': utils.NoneZeroFloat,
                                    '最低价': utils.NoneZeroFloat,
                                    '开盘价': utils.NoneZeroFloat,
                                    '前收盘': utils.NoneZeroFloat,
                                    '涨跌额': utils.Float,
                                    '涨跌幅': utils.Float,
                                    '换手率': utils.NoneZeroFloat,
                                    '成交量': utils.NoneZeroInt,
                                    '成交金额': utils.NoneZeroFloat,
                                    '总市值': utils.NoneZeroFloat,
                                    '流通市值': utils.NoneZeroFloat
                                    })
