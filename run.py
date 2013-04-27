# coding=utf8

from transform.models import HBaseClient
from parser.parser import ModelParser
import happybase

HBASE_HOST = '116.213.213.106'

TABLE_DCT = {
    'buzz': '%(url)s',
}

connection = happybase.Connection(
    host=HBASE_HOST,
)
connection.open()


def get(row_key, table_name='buzz', columns=None, table=None):
    ''' get one record from hbase by row_key '''
    #hc = HBaseClient(host=HBASE_HOST)
    #table = hc.connection.table(table_name)
    result = ModelParser().serialized(table_name, table.row(row_key, columns=columns))
    return result


def get_all(table_name='buzz', limit=1, row_start=None, row_stop=None, row_prefix=None, columns=None, filter=None, timestamp=None, include_timestamp=False, batch_size=1000):
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
    #hc = HBaseClient(host=HBASE_HOST)
    #hc.init_table('buzz', ['bz', ])

    '''
        在插入数据到HBase之后，插入一条记录到Redis(buzz_store_queue)
        记录格式如下: 
        
          '-'.join([ row_key, '%s_%s' % (uid, 'followRelations'), ]) 

        row_key => HBase中的row_key
        uid, followRelations => 会组合为文件名的一部分.
            建议采用 site(uid) + buzz(followRelations)来命名
    '''

    test_str = 'blablabla'
    test_data = {
        "title": 'testtile',
        "pan":    '0.123',
        "brief":  'testbreif',
        "url": 'http://Itmaybeatesturl',
        "create_at": 1364370727,
        "author": "testauthor",
        'site:': "testsite",
        "category": "testcategory",
        "comment_count": 1,
        "view_count": 2,
        "source": "testsource",
        "industry": "testindustry",
        'src': test_str,
    }
    table = connection.table('buzz')

    insert_data(
        test_data,
        test_data,
        table_name='buzz',
    )
    url = 'http://Itmaybeatesturl'

    print 'here is get whole line'
    print get(row_key=url, table=table)

    print 'here is only get src'
    print get(test_data.get('url'), columns=('src', ), table=table)
    #get_all()
