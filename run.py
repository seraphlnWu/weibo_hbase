# coding=utf8

from transform.models import HBaseClient
import happybase

HBASE_HOST = ''
HBASE_PORT = 0

TABLE_DCT = {
    'buzz': '%(url)s_%(create_at)s',
}


def insert_data(o_dict, table_name='buzz'):
    ''' insert test data '''
    hc = HBaseClient(host=HBASE_HOST, HBASE_PORT)
    table = hc.connection.table(table_name)
    table.put(
        TABLE_DCT.get(table_name) % o_dict,
        ModelParser().deserialized(
            table_name,
            o_dict,
        )
    )


def batch_insert(o_list, batch_size=1000, table_name='buzz'):
    ''' batch put data  '''
    hc = HBaseClient(host=HBASE_HOST, HBASE_PORT)
    table = hc.connection.table(table_name)
    with table.batch(batch_size=batch_size) as b:
        for cur_item in o_list:
            b.put(
                TABLE_DCT.get(table_name) % cur_item,
                ModelParser().deserialized(
                    table_name,
                    cur_item,
                )
            )


if __name__ == '__main__':
    pass
