# coding=utf8

from transform.models import HBaseClient
from parser.parser import ModelParser
import happybase

HBASE_HOST = '192.168.122.101'

TABLE_DCT = {
    'buzz': '%(url)s_%(create_at)s',
}


def insert_data(o_dict, default_value, table_name='buzz'):
    ''' insert test data '''
    hc = HBaseClient(host=HBASE_HOST)
    table = hc.connection.table(table_name)
    table.put(
        TABLE_DCT.get(table_name) % default_value,
        ModelParser().deserialized(
            table_name,
            o_dict,
        )
    )


def batch_insert(o_list, default_value, batch_size=1000, table_name='buzz'):
    ''' batch put data  '''
    hc = HBaseClient(host=HBASE_HOST)
    table = hc.connection.table(table_name)
    with table.batch(batch_size=batch_size) as b:
        for cur_item in o_list:
            b.put(
                TABLE_DCT.get(table_name) % default_value,
                ModelParser().deserialized(
                    table_name,
                    cur_item,
                )
            )


if __name__ == '__main__':
    import json
    #hc = HBaseClient(host=HBASE_HOST)
    #hc.init_table('buzz', ['bz', ])

    test_data = {
        "title": 'testtile',
        "pan":    '0.123',
        "brief":  'testbreif',
        "url": 'http://dealer.bitauto.com/100019023/news/201303/4212818.html',
        "create_at": 1364370727,
        "author": "testauthor",
        'site:': "testsite",
        "file_address": 'testfile_address',
        "category": "testcategory",
        "comment_count": 1,
        "view_count": 2,
        "source": "testsource",
        "industry": "testindustry",
    }
    result = json.dumps(test_data)
    insert_data({'content': result}, test_data)
