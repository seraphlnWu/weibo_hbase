# coding=utf8
'''
    test module.
'''

from smdata.models import InitTestData
from parser import ModelParser


def test_hbase_user():
    '''
        测试hbase init user
    '''
    hbase_instance = InitTestData('192.168.122.103')
    tmp_record = hbase_instance.insert_test_user()
    mp = ModelParser()
    result = mp.parse('user', tmp_record)
    print result
    import ipdb;ipdb.set_trace()
    print 'done!'

if __name__ == '__main__':
    test_hbase_user()
