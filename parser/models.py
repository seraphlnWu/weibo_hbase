# coding=utf8
'''
    model factary.
'''
from utils import parse_datetime_from_hbase
from utils import parse_boolean_from_hbase
from utils import parse_int_from_hbase
from utils import import_simplejson


USER_DATETIME_COLUMN_SET = {
    'user_tasks:flwr_update_time',
    'user_tasks:inf_update_time',
    'user_attrs:created_at',
    'user_attrs:join_at',
}

USER_BOOLEAN_COLUMN_SET = {
    'user_attrs:invalid',
    'user_attrs:sm_deleted',
    'user_attrs:verified',
}

USER_INT_COLUMN_SET = {
    'user_attrs:gender',
    'user_tasks:mention_since_id',
    'user_tasks:comment_since_id',
    'user_api:exp',
}

USER_LIST_COLUMN_SET = {
    'user_tasks:fuids',
    'user_tasks:task_list',
}

class ResultSet(list):
    '''A list like object that holds results from hbase'''


class Model(object):
    '''
        base model    
    '''
    def __init__(self, api=None):
        '''
        '''
        self._api = api

    def __getstate__(self):
        '''
        '''
        pickle = dict(self.__dict__)
        del pickle['_api']
        return pickle

    @classmethod
    def parse(cls, api, json):
        '''
            parse a HBase object into a model instance
        '''
        raise NotImplementedError

    @classmethod
    def parse_list(cls, api, json_list):
        '''
            parse a list of hbase objects into a result set of models instance
        '''
        results = ResultSet([cls.parse(api, obj) for obj in json_list])
        return results


class User(Model):
    '''
        parse Model structure
    '''
    @classmethod
    def parse(cls, api, json):
        '''
            inherit from Model and rewrite the parse func
        '''
        user = cls(api)
        for key, value in json.items():
            final_key = key.split(':')[1]
            if key in USER_DATETIME_COLUMN_SET:
                setattr(user, final_key, parse_datetime_from_hbase(value))
            elif key in USER_BOOLEAN_COLUMN_SET:
                setattr(user, final_key, parse_boolean_from_hbase(value))
            elif key in USER_INT_COLUMN_SET:
                setattr(user, final_key, parse_int_from_hbase(value))
            elif key in USER_LIST_COLUMN_SET:
                json = import_simplejson()
                try:
                    setattr(user, final_key, json.loads(value))
                except:
                    setattr(user, final_key, [])
            else:
                setattr(user, final_key, value)

        return user

    @classmethod
    def parse_list(cls, api, json_list):
        '''
            parse a list of model.
        '''
        if isinstance(json_list, list):
            item_list = json_list
        else:
            item_list = json_list['users']
    
        results = ResultSet([cls.parse(api, obj) for obj in item_list])
        return results


class ModelFactory(object):
    '''
        Used by parsers for creating instances of
        models. subclass this factary to add special
        models is also valid.
    '''
    user = User
