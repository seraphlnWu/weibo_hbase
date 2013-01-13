# coding=utf8

import pymongo
import happybase

from datetime import datetime

db = pymongo.Connection('localhost')['sandbox_mongo_5']


class HbaseInit(object):
    '''
        init hbase tables and columns.
    '''
    def __init__(self, hbase_host='localhost', autoconnect=True):
        '''
            init func.
        '''
        self.startup_hbase(hbase_host, autoconnect)

    def startup_hbase(self, hbase_host='localhost', autoconnect=True):
        '''
            连接hbase
        '''
        self.connection = happybase.Connection(
            hbase_host,
            autoconnect=autoconnect,
        )
        self.connection.open()
        

    def init_user(self):
        '''
            init the user table from mongodb
            and the mongodb user schema looked like below:

            row_key = weibo uid 
            column:family_api = { 
                rt, 
                exp,
                tok,
            }    
            column:family_user_attrs = { 
                screen_name,
                city,
                created_at,
                description,
                join_at,
                location,
                url,
                gender,
                province,
                profile_image_url,
                invalid,
                verified,
                sm_deleted,
            }    
            
            column:family_user_tasks_attrs = {
                column1 = task_list
                column2 = fuids
                column3 = comment_since_id
                column4 = flwr_update_time
                column5 = mension_since_id
                column6 = inf_update_time
            }
        '''
        self.connection.disable_table('users')
        self.connection.delete_table('users')

        self.connection.create_table(
            'users',
            {
                'user_api': dict(),
                'user_attrs': dict(),
                'user_tasks': dict(),
            }
        )

        table = self.connection.table('users')

        table.put(
            'test_row_1',
            {
                'user_api:rt': u'refresh_token',
                'user_api:exp': str(1234567890),
                'user_api:tok': u'access_token',
                'user_attrs:screen_name': u'seraphln',
                'user_attrs:city': u'shijiazhuang',
                'user_attrs:created_at': '2012-1-1',
                'user_attrs:description': u'blablabla', 
                'user_attrs:join_at': str('2013-1-1'),
                'user_attrs:location': u'somewhere',
                'user_attrs:url': 'http://www.seraphln.com',
                'user_attrs:gender': str(1),
                'user_attrs:province': u'hebei',
                'user_attrs:profile_image_url': 'http://www.seraphln.com/pic',
                'user_attrs:invalid': str(1),
                'user_attrs:verified': str(0),
                'user_attrs:sm_deleted': str(1),
                'user_tasks:task_list': str([1, 2, 3]),
                'user_tasks:fuids': str([{'a': 1,}, {'a': 2}]),
                'user_tasks:comment_since_id': str(12345),
                'user_tasks:flwr_update_time': '2013-1-2',
                'user_tasks:mention_since_id': str(23456),
                'user_tasks:inf_update_time': '2013-1-4',
            },
        )

        print table.row('test_row_1')
