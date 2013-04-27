# coding=utf8

from transform.models import HBaseClient
from parser.parser import ModelParser
import happybase

HBASE_HOST = '116.213.213.106'

TABLE_DCT = {
    'buzz': '%(url)s',
    'buzz_data': '%(url)s',
}

def get(row_key, table_name='buzz', columns=None, table=None):
    ''' get one record from hbase by row_key '''
    #hc = HBaseClient(host=HBASE_HOST)
    #table = hc.connection.table(table_name)
    result = ModelParser().serialized(table_name, table.row(row_key, columns=columns))
    return result


def get_all(table_name='buzz', limit=1):
    query_dict = {}
    if limit:
        query_dict.update({'limit': limit})
    else:
        pass
    hc = HBaseClient(host=HBASE_HOST)
    table = hc.connection.table(table_name)
    return ModelParser().serialized(
        table_name,
        table.scan(**query_dict),
    )
        #import ipdb;ipdb.set_trace()


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

    '''
    insert_data(
        {'content': result, 'src': test_str},
        test_data,
        table_name='buzz_data',
    )
    '''
    url = "http://aftersale.autov.com.cn/aftersale/modified/1203/35456.html"

    print get(url)
    #print get(test_data.get('ur'), 'src')
    #get_all()
    #print get('1720690654_3119384225', 'follow_relations')
