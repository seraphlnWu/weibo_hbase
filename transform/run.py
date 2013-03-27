# coding=utf8

'''
    run the tranform.
'''
import sys
from os.path import realpath, dirname
from os.path import join as path_join
sys.path.insert(0, realpath(path_join(dirname(__file__), '../')))

from multiprocessing import Process

import happybase
import pymongo

from models import HBaseClient

from config import HBASE_HOST
from config import MONGODB_HOST
from config import MONGODB_PORT
from config import MONGODB_NAME

from parser.parser import ModelParser

TABLE_DCT = {
    'followers': '%(id)s',
    'follow_relations': '%(user_id)s_%(follower_id)s',
    'comments': '%(sm_user_id)s_%(status_id)s_%(id)s',
    'reposts': '%(sm_user_id)s_%(retweeted_status_id)s_%(id)s',
    'mentions': '%(sm_user_id)s_%(_id)s',
    'mention_users': '%(sm_user_id)s_%(id)s',
    'status': '%(user_id)s_%(id)s',
    'buzz': '%(url)s_%(create_at)s',
}


connection = happybase.Connection(
    host=HBASE_HOST,
)
connection.open()

def get_hbase_connection():
    ''' get a hbase conn instance '''
    return HBaseClient(HBASE_HOST)


HBASE_CLIENT = HBaseClient(HBASE_HOST)
MONGO_INSTANCE = pymongo.Connection(MONGODB_HOST, MONGODB_PORT)[MONGODB_NAME]


def init_tables():
    ''' init whole hbase tables. '''
    HBASE_CLIENT.init_all_tables()


def insert_data(table_name, row_format):
    ''' insert test data '''
    model_parser = ModelParser()
    table = connection.table(table_name)
    cursor = MONGO_INSTANCE[table_name].find().limit(10)
    for cur_item in cursor:
        table.put(
            row_format % cur_item,
            model_parser.deserialized(
                table_name,
                cur_item,
            )
        )


def insert_all_data():
    ''' insert whole data '''
    for cur_table, row_format in TABLE_DCT.iteritems():
        insert_data(cur_table, row_format)
        p = Process(target=insert_data, args=[cur_table, row_format])
        p.start()
        p.join()

if __name__ == '__main__':
    #init_tables()
    #insert_all_data()
