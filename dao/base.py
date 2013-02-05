# coding=utf8
from weibo_dao.dao.utils import HBASE_INSTANCE
from weibo_dao.parser.utils import make_column_name
from weibo_dao.parser.parser import ModelParser

'''
    base class for data query.  
'''

class ResultList(list):
    ''' the return datastructure '''

class BaseQuery(object):
    ''' base class for data query '''

    tb_name = ''
    
    def __init__(self):
        ''' init func '''
        self.m_parser = ModelParser()
        self.table = HBASE_INSTANCE.table(self.tb_name)

    def query(self, **kwargs):
        '''
        query a bunch of results
        @row_start (str) – the row key to start at (inclusive)
        @row_stop (str) – the row key to stop at (exclusive)
        @row_prefix (str) – a prefix of the row key that must match
        @columns (list_or_tuple) – list of columns (optional)
        @filter (str) – a filter string (optional)
        @timestamp (int) – timestamp (optional)
        @include_timestamp (bool) – whether timestamps are returned
        @batch_size (int) – batch size for retrieving results
        '''
        
        return [self.m_parser.parse(self.tb_name, self.table.scan(**kwargs))]

    def query_one(self, id, **kwargs):
        '''
        query one result
        @id (str) – the row key
        @columns (list_or_tuple) – list of columns (optional)
        @timestamp (int) – timestamp (optional)
        @include_timestamp (bool) – whether timestamps are returned
        '''
        
        if 'columns' in kwargs:
            kwargs['columns'] = [make_column_name(self.tb_name, attr) for attr in kwargs['columns']]

        return self.m_parser.parse(
            self.tb_name,
            self.table.row(id, **kwargs)
        )

    def put_one(self, id, data, **kwargs):
        '''
        put / update one record
        @id (str) – the row key
        @data (dict) – the data to store
        @timestamp (int) – timestamp (optional)
        '''
        self.table.put(id, self.m_parser.de_parse(data), **kwargs)

    def delete(self, id, columns=None, **kwargs):
        '''
        delete records
        @id (str) – the row key
        @columns (list_or_tuple) – list of columns (optional)
        @timestamp (int) – timestamp (optional)
        '''
        
        columns = make_column_name(self.tb_name, columns) if columns else None
        self.table.delete(id, columns=columns, **kwargs)
        