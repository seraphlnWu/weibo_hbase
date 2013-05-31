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

from config import MONGODB_EXT_HOST
from config import MONGODB_EXT_PORT
from config import MONGODB_EXT_NAME

from parser.parser import ModelParser


TABLE_DCT = {
    'followers': '%(id)s',
    'follow_relations': '%(user_id)s_%(follower_id)s',
    'followbrand_flwrs': '%(id)s',
    'followbrand_flwr_relations': '%(followbrand_id)s_%(follower_id)s',
    'celebrity_flwrs': '%s(id)s',
    'celebrity_flwr_relations': '%(celebrity_id)s_%(folloewr_id)s',
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

MONGO_INSTANCE = pymongo.Connection(
    MONGODB_HOST,
    MONGODB_PORT,
)[MONGODB_NAME]

MONGO_EXT_INSTANCE = pymongo.Connection(
    MONGODB_EXT_HOST,
    MONGODB_EXT_PORT,
)[MONGODB_EXT_NAME]


def init_tables():
    ''' init whole hbase tables. '''
    HBASE_CLIENT.init_all_tables()


def insert_data(table_name, row_format):
    ''' insert test data '''
    model_parser = ModelParser()
    table = connection.table(table_name)

    if table_name in [
        'followers',
        'follow_relations',
        'followbrand_flwrs',
        'followbrand_flwr_relations',
    ]:
        db = MONGO_EXT_INSTANCE
    else:
        db = MONGO_INSTANCE

    cursor = db[table_name].find().limit(1000)


    length = 1000.0
    i = 0

    for cur_item in cursor:

        if table_name == 'follow_relations':
            try:
                tmp_flwr = db.followers.find_one({'_id': cur_item.get('follower_id')})
                cur_item.update(tmp_flwr)
            except:
                continue

        table.put(
            row_format % cur_item,
            model_parser.deserialized(table_name, cur_item),
        )
        i+=1
        if i % 100 == 0:
            print "=========>", "%02f" % (i/length, )


def insert_all_data():
    ''' insert whole data '''
    for cur_table, row_format in TABLE_DCT.iteritems():
        insert_data(cur_table, row_format)
        p = Process(target=insert_data, args=[cur_table, row_format])
        p.start()
        p.join()

if __name__ == '__main__':
    #init_tables()
    insert_all_data()
