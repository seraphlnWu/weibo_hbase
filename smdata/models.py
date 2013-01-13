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
        

    def init_module_structure(self):
        '''
            初始化表以及表结构 
        '''
        self.init_user()
        self.init_follow_relations()


    def init_test_data(self):
        '''
            初始化测试数据
        '''
        pass


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
        if self.connection.is_table_enabled('users'):
            self.connection.disable_table('users')
            self.connection.delete_table('users')
        else:
            pass

        self.connection.create_table(
            'users',
            {
                'user_api': dict(),
                'user_attrs': dict(),
                'user_tasks': dict(),
            }
        )


    def init_follow_relations(self):
        '''
            init the follow_relations func
            the data structure looks like below:

            row-key : uid_followerid
            column:family_follow_attributes: {
                created_at,
                sm_deleted,
                followers_count,
                friends_count,
                statuses_count,
                follower_id,
                sm_flwr_quality,
                activeness,
            }
            column:family: {
                comment_count,
                repost_count,
            }
        '''
        if self.connection.is_table_enabled('follow_relations'):
            self.connection.disable_table('follow_relations')
            self.connection.delete_table('follow_relations')
        else:
            pass
        
        self.connection.create_table(
            'follow_relations',
            {
                'follow_attrs': dict(),
                'task_attrs': dict(),
            }
        )

    def init_followers(self):
        '''
            init the followers table. 
        '''
        if self.connection.is_table_enabled('followers'):
            self.connection.disable_table('followers')
            self.connection.delete_table('followers')
        else:
            pass
        
        self.connection.create_table(
            'followers',
            {
                'follower_attrs': dict(),
            }
        )


class InitTestData(object):
    '''
        a test class.
        just for insert new test data.
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


    def insert_test_user(self):
        '''
            init a user test module.
        '''

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


    def init_test_follow_relations(self):
        '''
            insert a new follow_relations test data.
        '''
        table = self.connection.table('follow_relations')
        table.put(
            'test_follow_relation',
            {
                'follow_attrs:created_at': '2012-1-1',
                'follow_attrs:sm_deleted': '0',
                'follow_attrs:followers_count': '1234',
                'follow_attrs:friends_count': '2345',
                'follow_attrs:statuses_count': '3456',
                'follow_attrs:follower_id': '1234567890',
                'task_attrs:sm_flwr_quality': '23.13',
                'task_attrs:activeness': '30.31',
            },
        )

        print table.row('test_row_1')


    def init_test_followers(self):
        '''
            init a test followers 
        '''
        table = self.connection.table('followers')
        table.put(
            'followers',
            {
                'follower_attrs:id' : '1234567890',
                'follower_attrs:sm_update_time': '2012-11-12',
                'follower_attrs:name': 'seraphln',
                'follower_attrs:gender': '1',
                'follower_attrs:province': 'hebei',
                'follower_attrs:city': 'handan',
                'follower_attrs:location': 'somewhere',
                'follower_attrs:friends_count': '1234',
                'follower_attrs:follower_attrs': '2345',
                'follower_attrs:statuses_count': '3456',
                'follower_attrs:tags': '80hou,dota',
                'follower_attrs:domain': 'http://weibo.com/seraphln',
                'follower_attrs:description': 'blablabla',
                'follower_attrs:url': 'http://www.seraphln.com',
                'follower_attrs:profile_image_url': 'http://www.seraphln.com/pic',
                'follower_attrs:favourites_count': '4567',
                'follower_attrs:created_at': '2013-1-5',
                'follower_attrs:avatar': 'http://www.seraphln.com/avatar',
                'follower_attrs:fme': '1',
                'follower_attrs:vrson': 'I just want a V',
                'follower_attrs:online': '1',
                'follower_attrs:bfcnt': '15',
            }
        )
