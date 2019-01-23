# -*- coding: utf-8 -*-
from datasource import SHExchange,SZExchange,Sina
import asyncio


sh_bcodes = [ 'sh'+code for code in SHExchange.get_kzh_bonds()]
sz_bcodes = ['sz'+code for code in SZExchange.get_kzh_bonds()]
bcodes = sh_bcodes + sz_bcodes
ret = asyncio.run(Sina.aget_relative_codes(bcodes))
d = {bcodes[i]: ret[i] for i in range(len(ret)) if ret[i].startswith('s')}
print(1)
