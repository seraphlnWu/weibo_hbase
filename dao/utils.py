# coding=utf8
'''
    utils file.
    just for support functions and connections.
'''

import pymongo
import happybase

from datetime import datetime

from config import MONGODB_HOST
from config import MONGODB_PORT
from config import MONGODB_DBNAME

from config import HBASE_HOST


def get_db(
    mongo_host=MONGODB_PORT,
    mongo_port=MONGODB_PORT,
    mongo_dbname=MONGODB_DBNAME
):
    '''
        get a mongodb instance
    '''
    return pymongo.Connection(MONGODB_HOST, MONGODB_PORT)['sandbox_mongo_5']


def get_hbase_instance(hbase_host=HBASE_HOST, autoconnect=True):
    '''
        get a hbase instance
    '''
    return happybase.Connection(hbase_host, autoconnect=autoconnect)


MONGODB_INSTANCE = get_db()

HBASE_INSTANCE = get_hbase_instance()


def today_datetime():
    """ 返回今天的datetime时间 """
    return datetime.strptime(
        '%04d%02d%02d' % (lambda x: (x.year, x.month, x.day))(
            datetime.now()),
        '%Y%m%d')

def compare_value(o_value, default_value, logic_word='and'):
    ''' return o_value if o_value else default_value '''
    result = None
    if logic_word == 'and':
        result = o_value if o_value else default_value
    elif logic_word == 'not':
        result = o_value if not o_value else default_value

    return result
