# coding=utf8
'''
    transform models.
'''

import pymongo
import happybase

from parser.parser import ModelParser

MONGO_INSTANCE = pymongo.Connection('localhost')['sandbox_mongo_5']


class HbaseInit(object):
    '''
        init hbase tables and columns.
    '''
    def __init__(self, hbase_host='localhost', autoconnect=True):
        '''
            init func.
        '''
        self.connection = None
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
        self.init_followers()


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
        self.connection = None
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

        model_parser = ModelParser()
        table = self.connection.table('users')
        users = MONGO_INSTANCE.users.find()
        for user in users:
            print user.get('_id'), model_parser.de_parse('users', 'user', user)
            table.put(
                str(user.get('_id')),
                model_parser.de_parse('attrs', 'user', user),
            )


    def init_test_follow_relations(self):
        '''
            insert a new follow_relations test data.
        '''
        model_parser = ModelParser()
        table = self.connection.table('follow_relations')
        follow_relations = MONGO_INSTANCE.follow_relations.find()
        for cur_relation in follow_relations:
            print cur_relation.get('user_id'), model_parser.de_parse('follow_relation', 'follow_relation', cur_relation)


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
                'follower_attrs:profile_image_url': 'http://www.seraphln.com/',
                'follower_attrs:favourites_count': '4567',
                'follower_attrs:created_at': '2013-1-5',
                'follower_attrs:avatar': 'http://www.seraphln.com/avatar',
                'follower_attrs:fme': '1',
                'follower_attrs:vrson': 'I just want a V',
                'follower_attrs:online': '1',
                'follower_attrs:bfcnt': '15',
            }
        )
