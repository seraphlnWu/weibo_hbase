# coding=utf8
'''
common functions here.
'''
from datetime import datetime

from config import USER_TASKS_COLUMN_FAMILY_SET
from config import USER_ATTRS_COLUMN_FAMILY_SET
from config import USER_API_COLUMN_FAMILY_SET

from config import FOLLOW_RELATION_FOLLOW_ATTRS_SET
from config import FOLLOW_RELATION_FOLLOW_TASK_SET

import sm_log

logger = sm_log.get_logger('weibo_hbase_utils')


def parse_datetime_from_hbase(o_datetime):
    '''
        convert the datetime type string which
        from hbase to a datetime
    '''
    result = None
    try:
        result = datetime.strptime(o_datetime, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError as msg:
        logger.info(str(msg))
        try:
            result = datetime.strptime(o_datetime, '%Y-%m-%d %H:%M:%S')
        except ValueError as msg:
            logger.info(str(msg))

    return result


def parse_datetime_into_hbase(o_datetime):
    '''
        parse the datetime to hbase support type
    '''
    return str(o_datetime)

def parse_boolean_into_hbase(o_value):
    '''
        parse the boolean to hbase support type
    '''
    flag = 1 if o_value else 0
    return str(flag)

def parse_int_into_hbase(o_value):
    '''
        parse the int to hbase support type
    '''
    return str(o_value)


def parse_boolean_from_hbase(o_value):
    '''
        convert the boolean type string which
        from hbase to a datetime
    '''
    result = True if int(o_value) else False
    return result


def parse_int_from_hbase(o_value):
    '''
        convert the int type string which
        from hbase to a datetime
    '''
    result = int(o_value)
    return result


def import_simplejson():
    '''
        try to import a simplejson module.
    '''
    try:
        import simplejson as json
    except ImportError:
        try:
            import json  # Python 2.6+
        except ImportError:
            try:
                from django.utils import simplejson as json  # Google App Engine
            except ImportError:
                raise ImportError, "Can't load a json library"
 
    return json


def get_user_column_prefix(key):
    '''
        get the column name
    '''
    name = None
    if key in USER_API_COLUMN_FAMILY_SET:
        name = 'user_api'
    elif key in USER_ATTRS_COLUMN_FAMILY_SET:
        name = 'user_attrs'
    elif key in USER_TASKS_COLUMN_FAMILY_SET:
        name = 'user_tasks'
    else:
        print 'blablabla'
        print key
        raise NotImplementedError

    return name

def get_follow_relation_column_prefix(key):
    '''
        get the follow_relation column name
    '''
    name = None
    if key in FOLLOWBRAND_FOLLOW_ATTRS_SET:
        name = 'follow_attrs'
    elif key in FOLLOWBRAND_FOLLOW_TASK_SET:
        name = 'task_attrs'
    else:
        print 'blablabla'
        print key
        raise NotImplementedError

    return name


def get_followers_column_prefix(key):
    '''
        get followers column name
    '''
    return 'follower_attrs'


def make_column_name(prefix, attr):
    '''
        return a column name
    '''
    proc_dict = {
        'users': get_user_column_prefix,
        'follow_relation': get_follow_relation_column_prefix,
        'followers': get_followers_column_prefix,
    }

    return ':'.join([proc_dict[prefix](attr), attr])


def convert_data(o_value):
    '''
        convert the data
    '''
    if isinstance(o_value, unicode):
        return o_value.encode('utf8')
    else:
        return str(o_value)
