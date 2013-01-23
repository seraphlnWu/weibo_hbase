# coding=utf8

'''
    this is a dao for whole weibo master user authorities.
'''

from utils import MONGODB_INSTANCE

from user_dao import get_user_by_id


def get_write_perm(username):
    ''' check the given user write permission '''
    result = None

    tmp_result = MONGODB_INSTANCE.write_perm.find_one({'_id': username})
    if tmp_result:
        result = tmp_result.get('uw', 0)
    else:
        result = 0

    return result


def get_vidt(uid):
    ''' check the given uid had the vidt authority '''
    result = False

    current_user = get_user_by_id(uid)
    if current_user:
        result = current_user.get('vidt', False)
    else:
        pass

    return result


def get_perm(username):
    ''' 查看一个用户是否拥有数据中心的权限 '''
    return MONGODB_INSTANCE.dt_perm.find_one({'username': username}) or {}


def get_pm_benchmark(username):
    ''' 查看一个中户是否拥有benchmark的权限 '''
    tmp_res = MONGODB_INSTANCE.pm_benchmark.find_one({'_id': username})
    return tmp_res.get('uw', 0) if tmp_res else 0
