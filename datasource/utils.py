import numpy as np
import pandas as pd
import decimal
from decimal import Decimal


def add_prefix(code, sh, sz):
    if code[0] == '6':
        return sh + code
    elif code[0] == '0' or code[0] == '3':
        return sz + code
    raise RuntimeError('invalid code %s' % code)


def add_sina_prefix(code):
    return add_prefix(code, 'sh', 'sz')


def add_wangyi_prefix(code):
    return add_prefix(code, '0', '1')


class CSVHelper:
    def __init__(self, s):
        self.__s = s
        self.__len = len(s)
        self.__newLinePos = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.__newLinePos == self.__len:
            raise StopIteration
        startPos = self.__newLinePos
        while self.__s[self.__newLinePos] != '\n':
            self.__newLinePos += 1
        self.__newLinePos += 1
        return self.__s[startPos:self.__newLinePos]


class WYRCSVHelper:
    def __init__(self, s, header_length):
        self.__s = s
        self._hl = header_length
        self.__len = len(s)
        self.__newLinePos = self.__len-2

    def __iter__(self):
        return self

    def __next__(self):
        if self.__newLinePos < self._hl:
            raise StopIteration
        endPos = self.__newLinePos
        while self.__s[self.__newLinePos] != '\n':
            self.__newLinePos -= 1
        self.__newLinePos -= 1
        return self.__s[self.__newLinePos+2:endPos+1]


class Float:
    def __new__(cls, *args, **kwargs):
        if 'None' == args[0]:
            return np.nan
        else:
            return np.float(*args, **kwargs)


class NoneZeroFloat:
    def __new__(cls, *args, **kwargs):
        if args[0] == '0.0' or args[0] == 'None':
            return np.nan
        else:
            return np.float(*args, **kwargs)
        
        
class NoneZeroInt:
    def __new__(cls, *args, **kwargs):
        if args[0] == '0':
            return np.nan
        else:
            return int(*args, **kwargs)

def ztj(p):
    context = decimal.getcontext()
    context.prec = 6
    context.rounding = decimal.ROUND_HALF_UP
    dc = Decimal(p)
    d = dc*Decimal(0.1)
    ret = dc+d
    return float(ret.quantize(Decimal('0.00',context)))
