# coding=utf8
'''
    utils file.
    just for support functions and connections.
'''

import pymongo
import happybase

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
