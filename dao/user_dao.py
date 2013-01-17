# coding=utf8

from utils import MONGODB_INSTANCE
from utils import HBASE_INSTANCE

USER_TABLE = HBASE_INSTANCE.table('users')

def get_users():
    '''
        获取全部的用户信息列表    
    '''
    return USER_TABLE.scan()


def get_user_by_id(uid):
    '''
        根据传入的uid获取相应的user信息
    '''
    return USER_TABLE.row(str(uid))
