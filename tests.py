# coding=utf8
'''
    test module.
'''
import happybase

from transform.models import InitTestData
from transform.models import HbaseInit
from parser.parser import ModelParser


def test_hbase_user():
    '''
        测试hbase init user
    '''
    hbase_instance = InitTestData('192.168.122.101')
    #hbase_instance.test_select_with_prefix()
    #hi = HbaseInit('192.168.122.101')
    #hi.test_select_with_prefix()
    #hi.init_followers()
    hbase_instance.insert_test_followers()
    #print 'done'
    #tmp_record = hbase_instance.insert_test_followers()
    '''
    mp = ModelParser()
    result = mp.parse('followers', tmp_record)
    print result
    import ipdb;ipdb.set_trace()
    print 'done!'
    '''


if __name__ == '__main__':
    '''

    '''
    #import profile
    #profile.run("test_hbase_user()")
    test_hbase_user()
