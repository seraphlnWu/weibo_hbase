# coding=utf8

from datetime import datetime
import time

def convert_datetime_to_time(o_datetime):
    ''' convert a datetime object to timestamp '''
    if not isinstance(o_datetime, datetime): 
        raise TypeError, 'The type is not correctly'
    else:
        pass
    return int(time.mktime(o_datetime.timetuple()))
