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
import json


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
    
    @classmethod
    def get_kzh_bonds(cls):
        _headers = {'Referer': 'http://www.sse.com.cn/assortment/bonds/list/'}
        url = 'http://query.sse.com.cn/commonQuery.do?jsonCallBack=jsonpCallback99006&isPagination=true&sqlId=COMMON_BOND_KZZFLZ_ALL&pageHelp.pageSize=1000&pageHelp.cacheSize=1&pageHelp.pageNo=1&pageHelp.beginPage=1&pagecache=false&BONDCODE=&KZZ=1&_=1548125040324'
        response = requests.get(url, headers=_headers)
        response.raise_for_status()
        r = json.loads(response.text[19:-1])['result']
        now = datetime.now()
        today = now.date()
        stoday = today.strftime('%Y-%m-%d')
        return [d['BOND_CODE'] for d in r if d['END_DATE']>stoday]
 
    
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
    __indexes_folder = 'E:/quant/data/wangyi/indexes/'
    __indexes = ['0000001', '1399001', '1399005', '1399006']
    __stock_converter = {
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
    }
    __index_converter = {
            '日期': lambda s_date: datetime.strptime(s_date, '%Y-%m-%d'),
            '收盘价': utils.NoneZeroFloat,
            '最高价': utils.NoneZeroFloat,
            '最低价': utils.NoneZeroFloat,
            '开盘价': utils.NoneZeroFloat,
            '前收盘': utils.NoneZeroFloat,
            '涨跌额': utils.Float,
            '涨跌幅': utils.Float,
            '成交量': utils.NoneZeroInt,
            '成交金额': utils.NoneZeroFloat
        }

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
    def make_index_url(cls, code, end_date, start_date):
        url = "http://quotes.money.163.com/service/chddata.html? code=%s&start=%s&end=%s&" \
              "fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER" % (code, start_date, end_date)
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
    def get_index_day_data(cls, code, end_date, start_date=__open_date):
        url = cls.make_index_url(code, end_date, start_date)
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
        return (pd.read_csv(cls.__stocks_folder + name, encoding='gbk', index_col=0, converters=cls.__stock_converter)
                for name in os.listdir(cls.__stocks_folder))

    @classmethod
    def read_stock(cls, code):
        names = (name for name in os.listdir(cls.__stocks_folder))
        for name in names:
            if code in name:
                return pd.read_csv(cls.__stocks_folder+name, encoding='gbk', index_col=0,
                                   converters=cls.__stock_converter)

    @classmethod
    def read_index(cls, code):
        return pd.read_csv(cls.__indexes_folder+code+'.csv', encoding='gbk', index_col=0,
                           converters=cls.__index_converter)

    @classmethod
    def read_one_column(cls, col_name='收盘价'):
        ret = []
        for name in os.listdir(cls.__stocks_folder):
            df = pd.read_csv(cls.__stocks_folder+name, encoding='gbk', index_col=0, converters=cls.__stock_converter)
            series = df[col_name]
            series.name = name[11:17]
            ret.append(series)
        return pd.DataFrame(ret).T

    @classmethod
    def download_indexes(cls):
        dt = datetime.now()
        _date = dt.date()
        if dt < datetime(_date.year, _date.month, _date.day, 15, 30, 0):
            _date -= timedelta(1)
        for index in cls.__indexes:
            content = cls.get_index_day_data(index, _date.strftime('%Y%m%d'))
            with open(cls.__indexes_folder + '%s.csv' % index[1:], 'w', newline='') as f:
                f.write(content[0:48])
                helper = utils.WYRCSVHelper(content, 48)
                f.writelines(helper)


class Sina:
    __session = None
    __url = None
    
    class Quote:
        def __init__(self, s):
            t = s.split(',')
            self.__code = t[0][13:19]
            self.__price = t[3]
            
            self.__bid1 = t[11]
            self.__bid1_vol = t[10]
            self.__bid2 = t[13]
            self.__bid2_vol = t[12]
            self.__bid3 = t[15]
            self.__bid3_vol = t[14]
            self.__bid4 = t[17]
            self.__bid4_vol = t[16]
            self.__bid5 = t[19]
            self.__bid5_vol = t[18]
            
            self.__ask1 = t[21]
            self.__ask1_vol = t[20]
            self.__ask2 = t[23]
            self.__ask2_vol = t[22]
            self.__ask3 = t[25]
            self.__ask3_vol = t[24]
            self.__ask4 = t[27]
            self.__ask4_vol = t[26]
            self.__ask5 = t[29]
            self.__ask5_vol = t[28]
            
        @property
        def code(self):
            return self.__code
        
        @property
        def price(self):
            return self.__price
        
        @property
        def bid1(self):
            return self.__bid1
        
        @property
        def bid2(self):
            return self.__bid2    
        @property
        def bid3(self):
            return self.__bid3
        
        @property
        def bid4(self):
            return self.__bid4
        @property
        def bid5(self):
            return self.__bid5
        
        @property
        def ask1(self):
            return self.__ask1
        
        @property
        def ask2(self):
            return self.__ask2  

        @property
        def ask3(self):
            return self.__ask3
        
        @property
        def ask4(self):
            return self.__ask4 
        
        @property
        def ask5(self):
            return self.__ask5
        
        @property
        def bid1_vol(self):
            return self.__bid1_vol
        
        @property
        def bid2_vol(self):
            return self.__bid2_vol
        
        @property
        def bid3_vol(self):
            return self.__bid3_vol
        
        @property
        def bid4_vol(self):
            return self.__bid4_vol
        
        @property
        def bid5_vol(self):
            return self.__bid5_vol
        
        @property
        def ask1_vol(self):
            return self.__ask1_vol
        
        @property
        def ask2_vol(self):
            return self.__ask2_vol
        
        @property
        def ask3_vol(self):
            return self.__ask3_vol
        
        @property
        def ask4_vol(self):
            return self.__ask4_vol
        
        @property
        def ask5_vol(self):
            return self.__ask5_vol
        
        
    @classmethod
    def subscribe(cls, iterateable):
        cls.__url = 'http://hq.sinajs.cn/list='+','.join(iterateable)
    
    @classmethod
    def get_rt_quote(cls):
        if cls.__session is None:
            cls.__session = requests.Session()
        response = cls.__session.get(cls.__url)
        response.raise_for_status()
        lines = response.text.splitlines()
        ret = []
        for line in lines:
            if len(line) > 100:
                ret.append(cls.Quote(line))
        return ret
    
    @classmethod
    def get_relative_code(cls,bond_code):
        url = 'http://money.finance.sina.com.cn/bond/quotes/{0}.html'.format(bond_code)
        for i in range(3):
            response = requests.get(url,timeout=3)
            if response.status_code==200:
                break;
        response.raise_for_status()
        response.encoding = 'gbk'
        t = response.text
        pos = t.find('relatedStock')
        return t[pos+16:pos+24]
    
    @classmethod
    async def aget_relative_code(cls, bcode):
        url = 'http://money.finance.sina.com.cn/bond/quotes/{0}.html'.format(bcode)
        async with cls.__session.get(url) as response:
             t = await response.text(encoding='gbk')
             pos = t.find('relatedStock')
             return t[pos+16:pos+24]
    
    @classmethod
    async def aget_relative_codes(cls, bcodes):
        async with aiohttp.ClientSession() as client:
            cls.__session = client
            ret = await asyncio.gather(*tuple(Sina.aget_relative_code(code) for code in bcodes))
        del cls.__session
        return ret
            
    
    
        
        
            
        