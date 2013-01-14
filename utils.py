# coding=utf8
'''
common functions here.
'''
from datetime import datetime

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
