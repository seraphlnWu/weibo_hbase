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

