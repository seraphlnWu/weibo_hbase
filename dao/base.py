# coding=utf8

'''
    base class for data query.  
'''

class ResultList(list):
    ''' the return datastructure '''

class BaseQuery(object):
    ''' base class for data query '''

    def query(self, *args, **kwargs):
        ''' query with args or kwargs '''
        raise NotImplementedError

    def query_one(self, *args, **kwargs):
        ''' query one result with kwargs '''
        raise NotImplementedError

    def put_one(self, *args, **kwargs):
        ''' put one record with kwargs '''
        raise NotImplementedError

    def delete(self, *args, **kwargs):
        ''' delete records with kwargs '''
        raise NotImplementedError
