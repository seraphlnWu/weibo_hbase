# coding=utf8
'''
common functions here.
'''
from datetime import datetime

from struct import pack
from struct import unpack

import sm_log

logger = sm_log.get_logger('weibo_hbase_utils')


def convert_data(o_value):
    ''' convert the given value to utf8 '''
    if isinstance(o_value, unicode):
        return o_value.encode('utf8')
    else:
        return str(o_value)


def parse_datetime_from_hbase(o_datetime):
    ''' convert the give struct bytes from HBase to datetime. '''
    result = None
    try:
        result = datetime.strptime(o_datetime, '%Y-%m-%d %H:%M:%S')
    except ValueError as msg:
        logger.info(str(msg))

    return result


def parse_boolean_from_hbase(o_value):
    ''' convert the bytes to boolean '''
    return unpack('b', o_value)[0]


def parse_int_from_hbase(o_value):
    ''' convert the bytes to int '''
    return unpack('>q', o_value)[0]


def parse_float_from_hbase(o_value):
    ''' convert the bytes to float '''
    return unpack('f', o_value)[0]


def parse_list_from_hbase(o_value):
    ''' the strings with comma to be a list '''
    return o_value.split(',')


def parse_datetime_into_hbase(o_datetime):
    ''' parse the datetime to hbase support type '''

    result = None
    if isinstance(o_datetime, unicode):
        o_datetime = format_date(o_datetime)
    else:
        pass
    result = o_datetime.strftime('%Y-%m-%d %H:%M:%S')

    return result


def parse_boolean_into_hbase(o_value):
    ''' parse the boolean value to bytes'''
    return pack('b', o_value)


def parse_int_into_hbase(o_value):
    ''' parse the int to bytes '''
    if o_value:
        o_value = int(o_value)
    else:
        o_value = 0
    return pack('>q', o_value)


def parse_float_into_hbase(o_value):
    ''' convert the bytes to float '''
    if o_value:
        o_value = float(o_value)
    else:
        o_value = 0.0
    return pack('f', o_value)


def parse_list_into_hbase(o_value):
    ''' parse the list to strings with comma '''
    return ','.join(map(convert_data, o_value))


DEPARSE_MAPPER = {
    'int': parse_int_into_hbase,
    'boolean': parse_boolean_into_hbase,
    'datetime': parse_datetime_into_hbase,
    'list': parse_list_into_hbase,
    'float': parse_float_into_hbase,
    'string': convert_data,
}

PARSE_MAPPER = {
    'int': parse_int_from_hbase,
    'boolean': parse_boolean_from_hbase,
    'datetime': parse_datetime_from_hbase,
    'list': parse_list_from_hbase,
    'float': parse_float_from_hbase,
    'string': convert_data,
}


def reverse_the_column_to_key(o_dict):
    ''' reverse the column to dict key '''
    return (
        o_dict,
        dict([(
            v.get('column_name'),
            {'column_name': k, 'type': v.get('type')}
        ) for k, v in o_dict.iteritems()]),
    )


def format_date(created_at):
    """ 格式化创建时间 """
    if isinstance(created_at, unicode):
        return datetime.strptime(created_at, '%a %b %d %H:%M:%S +%f %Y')
    else:
        return created_at
