# coding=utf8
'''
    test module.
'''

from transform.models import InitTestData
from transform.models import HbaseInit
from parser.parser import ModelParser


def test_hbase_user():
    '''
        测试hbase init user
    '''
    hbase_instance = InitTestData('192.168.122.103')
    hi = HbaseInit('192.168.122.103')
    hi.init_comments()
    hbase_instance.insert_test_comments()
    tmp_record = hbase_instance.insert_test_comments()

if __name__ == '__main__':
    test_hbase_user()
