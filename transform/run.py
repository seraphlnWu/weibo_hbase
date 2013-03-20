# coding=utf8

'''
    run the tranform.
'''
import sys, os
from os.path import realpath, dirname
from os.path import join as path_join
sys.path.insert(0, realpath(path_join(dirname(__file__), '../')))

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
}


connection = happybase.Connection(
    host=HBASE_HOST,
)
connection.open()

def get_hbase_connection():
    return HBaseClient(HBASE_HOST)


HBASE_CLIENT = HBaseClient(HBASE_HOST)
MONGO_INSTANCE = pymongo.Connection('localhost')['sandbox_mongo_5']


def init_tables():
    ''' '''
    HBASE_CLIENT.init_all_tables()


def insert_data(table_name, row_format):
    ''' insert test data '''
    model_parser = ModelParser()
    print table_name
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
    for cur_table, row_format in TABLE_DCT.iteritems():
        insert_data(cur_table, row_format)

if __name__ == '__main__':
    init_tables()
    #insert_test_data('followers')
    insert_all_data()