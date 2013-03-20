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



    def init_follow_relations(self):
        flag = False
        try:
            flag = self.connection.is_table_enabled('follow_relations')
        except:
            pass

        if flag:
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
        flag = False
        try:
            flag = self.connection.is_table_enabled('followers')
        except:
            pass

        if flag:
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


    def insert_test_follow_relations(self):
        '''
            insert a new follow_relations test data.
        '''
        model_parser = ModelParser()
        table = self.connection.table('follow_relations')
        follow_relations = MONGO_INSTANCE.follow_relations.find()
        from datetime import datetime
        a = datetime.now()
        for cur_relation in follow_relations:
            print cur_relation.get('user_id'), model_parser.de_parse('follow_relation', 'follow_relation', cur_relation)
            table.put(
                '_'.join([
                    str(cur_relation.get('user_id')),
                    str(cur_relation.get('follower_id')),
                ]),
                model_parser.de_parse(
                    'follow_relation',
                    'follow_relation',
                    cur_relation,
                )
            )
        print follow_relations.count()
        print datetime.now() - a


    def insert_test_followers(self):
        '''
            init a test followers 
        '''
        model_parser = ModelParser()
        table = self.connection.table('followers')
        followers = MONGO_INSTANCE.followers.find()
        from datetime import datetime
        a = datetime.now()
        for cur_follower in followers:
            print cur_follower.get('_id'), model_parser.de_parse('followers', 'followers', cur_follower)
            table.put(
                str(cur_follower.get('_id')),
                model_parser.de_parse(
                    'followers',
                    'followers',
                    cur_follower,
                )
            )
        print followers.count()
        print datetime.now() - a
