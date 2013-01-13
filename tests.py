# coding=utf8
'''
    test module.
'''

from smdata.models import HbaseInit


def test_hbase_user():
    '''
        测试hbase init user
    '''
    hbase_instance = HbaseInit('192.168.122.103')
    hbase_instance.init_user()


if __name__ == '__main__':
    '''
        main function
    '''
    test_hbase_user()
