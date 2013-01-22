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

    def query_one(self, **kwargs):
        ''' query one result with kwargs '''
        raise NotImplementedError

    def put_one(self, **kwargs):
        ''' put one record with kwargs '''
        raise NotImplementedError

    def delete(self, **kwargs):
        ''' delete records with kwargs '''
        raise NotImplementedError
