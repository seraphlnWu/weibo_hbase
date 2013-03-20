# coding=utf8
'''
    transform models.
'''
import time

from datetime import datetime

import pymongo
import happybase

from parser.parser import ModelParser

from transform.config import TABLE_CF_MAPPER


class HBaseClient(object):
    ''' init hbase tables and columns. '''

    def __init__(
        self,
        host='localhost',
        port=9090,
        autoconnect=True,
        table_prefix=None,
        table_prefix_separator='_',
        compat='0.92',
        transport='buffered',
    ):
        ''' init the connection '''
        self.host = host
        self.port = port
        self.autoconnect = autoconnect
        self.table_prefix = table_prefix
        self.table_prefix_separator = table_prefix_separator
        self.compat = compat
        self.transport = transport

        self.connection = None
        self.table = None

        self._startup_hbase()

    def _startup_hbase(self):
        ''' 连接hbase '''
        self.connection = happybase.Connection(
            host=self.host,
            port=self.port,
            autoconnect=self.autoconnect,
            table_prefix=self.table_prefix,
            table_prefix_separator=self.table_prefix_separator,
            compat=self.compat,
            transport=self.transport,
        )
        self.connection.open()

    def close(self):
        ''' close the connection '''
        self.connection.close()


    def init_table(self, table_name, cf):
        '''
            init a table.
            if the table does not exists. create it.

            @table_name => the table to init.
            @cf => the column family of this table.
        '''
        flag = False
        try:
            flag = self.connection.is_table_enabled(table_name)
        except:
            pass

        '''
        if flag:
            print 'Current table is exists.'
        else:
        '''
        # here for testing, so, if the table is exists
        # I will drop it and recreate a new one.
        print table_name
        try:
            self.connection.disable_table(table_name)
            self.connection.delete_table(table_name)
        except:
            pass

        print '%s will be created in ...' % table_name
        for i in range(3, 0, -1):
            print '%d...' % i
            time.sleep(1)

        self.connection.create_table(
            table_name,
            dict([(x, dict()) for x in cf]),  # for python 2.6-
        )

    def init_all_tables(self):
        ''' 初始化表以及表结构 '''
        for table_name, cf in TABLE_CF_MAPPER.iteritems():
            self.init_table(table_name, cf)


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
        a = datetime.now()
        for user in users:
            table.put(
                str(user.get('_id')),
                model_parser.de_parse('users', 'user', user),
            )
            if user.get('id') == 1969092405:
                print 'blablabla'
        print datetime.now() - a


    def insert_test_follow_relations(self):
        '''
            insert a new follow_relations test data.
        '''
        model_parser = ModelParser()
        table = self.connection.table('follow_relations')
        follow_relations = MONGO_INSTANCE.follow_relations.find()
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
        BATCH_SIZE = 10000
        model_parser = ModelParser()
        table = self.connection.table('follower_test')
        follow_relations = MONGO_INSTANCE.follow_relations.find().skip(1143179).limit(5000000)
        #followers = MONGO_INSTANCE.followers.find()
        #b = table.batch(batch_size=BATCH_SIZE)
        a = datetime.now()
        for cur_follow_relation in follow_relations:
            #print cur_follower.get('_id'), model_parser.de_parse('followers', 'followers', cur_follower)
            uid = cur_follow_relation.get('user_id')
            follower_id = cur_follow_relation.get('follower_id')
            cur_follower = MONGO_INSTANCE.followers.find_one({'_id': follower_id})
            if not cur_follower:
                print uid, follower_id, 'does not exists'
                continue
            cur_follower.update({'uid': uid, 'follower_id': follower_id})
            tmp_model = model_parser.de_parse(
                'followers',
                'followers',
                cur_follower,
            )
            table.put(
                '_'.join(map(str, [uid, follower_id])),
                tmp_model,
            )
        print datetime.now() - a


    def insert_test_attrs(self):
        '''
            init a test followers 
        '''
        BATCH_SIZE = 10000
        table = self.connection.table('test_put')
        followers = MONGO_INSTANCE.followers.find()
        followers_ids = [x.get('_id') for x in followers]
        #b = table.batch(batch_size=BATCH_SIZE)
        from datetime import datetime
        a = datetime.now()
        for cur_id in followers_ids:
            table.put(
                str(cur_id),
                {'some_attr:attr': "blablabla"},
            )
        print followers.count()
        print datetime.now() - a
