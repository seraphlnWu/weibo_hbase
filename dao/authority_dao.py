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
