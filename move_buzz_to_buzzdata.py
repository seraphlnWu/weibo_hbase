# coding=utf8

from run import get_all
from run import insert_data
from run import get

import cPickle


def move_data():
    for i, cur_record in enumerate(get_all(limit=None)):
        print i
        tmp_result = cPickle.loads(cur_record.get('content')) 
        test_data = {'url': tmp_result.get('url')}
        src = tmp_result.pop('zipped_page')

        insert_data({'content': cPickle.dumps(tmp_result), 'src': cPickle.dumps(src)}, test_data, table_name='buzz_data')

if __name__ == '__main__':
    move_data()
