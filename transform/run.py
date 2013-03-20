# coding=utf8

'''
    run the tranform.
'''
import sys, os
from os.path import realpath, dirname
from os.path import join as path_join
sys.path.insert(0, realpath(path_join(dirname(__file__), '../')))

import pymongo

from models import HBaseClient

from config import HBASE_HOST
from config import MONGODB_HOST
from config import MONGODB_PORT
from config import MONGODB_NAME

from parser.parser import ModelParser


def get_hbase_connection():
    return HBaseClient(HBASE_HOST)


#HBASE_CLIENT = get_hbase_connection()
MONGO_INSTANCE = pymongo.Connection('localhost')['sandbox_mongo_5']


def init_tables():
    ''' '''
    hbase_client.init_all_tables()


def insert_test_data(table_name):
    ''' insert test data '''
    model_parser = ModelParser()
    cursor = MONGO_INSTANCE[table_name].find().limit(10)
    for cur_item in cursor:
        tmp_record = model_parser.deserialized(
            table_name,
            cur_item, 
        )
        print tmp_record
    

def insert_follow_relations():
    pass


def insert_comments():
    pass


def insert_reposts():
    pass


def inesrt_mentions():
    pass


def insert_mention_users():
    pass


def insert_status():
    pass


if __name__ == '__main__':
    insert_test_data('followers')
    pass
