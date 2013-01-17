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
    hi.init_followers()
    tmp_record = hbase_instance.insert_test_followers()
    mp = ModelParser()
    result = mp.parse('followers', tmp_record)
    print result
    import ipdb;ipdb.set_trace()
    print 'done!'

if __name__ == '__main__':
    test_hbase_user()
