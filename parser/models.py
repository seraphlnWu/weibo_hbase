# coding=utf8
'''
    model factary.
'''
from utils import parse_datetime_from_hbase
from utils import parse_boolean_from_hbase
from utils import parse_int_from_hbase
from utils import import_simplejson
from utils import make_column_name
from utils import parse_datetime_into_hbase
from utils import parse_boolean_into_hbase
from utils import parse_int_into_hbase
from utils import convert_data

from config import USER_DATETIME_COLUMN_SET
from config import USER_BOOLEAN_COLUMN_SET
from config import USER_INT_COLUMN_SET
from config import USER_LIST_COLUMN_SET



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
    def de_parse(cls, prefix, json):
        '''
            de parse a mongodb dict into Hbase type strcture
        '''
        raise NotImplementedError

    @classmethod
    def parse_list(cls, api, json_list):
        '''
            parse a list of hbase objects into a result set of models instance
        '''
        results = ResultSet([cls.parse(api, obj) for obj in json_list])
        return results

    @classmethod
    def de_parse_list(cls, prefix, json_list):
        '''
            de parse a list of mongodb objects to a set of hbase type
        '''
        results = ResultSet([cls.de_parse(api, obj) for obj in json_list])
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
    def de_parse(cls, prefix, json):
        '''
            de parse
        '''
        result_dict = {}
        for key, value in json.iteritems():
            if key in {'_id', 'id'}:
                pass
            else:
                key_name = make_column_name(prefix, key)
                print key_name, value
                if key_name in USER_DATETIME_COLUMN_SET:
                    result_dict[key_name] = parse_datetime_into_hbase(value)
                elif key_name in USER_BOOLEAN_COLUMN_SET:
                    result_dict[key_name] = parse_boolean_into_hbase(value)
                elif key_name in USER_INT_COLUMN_SET:
                    result_dict[key_name] = parse_int_into_hbase(value)
                elif key_name in USER_LIST_COLUMN_SET:
                    json = import_simplejson()
                    result_dict[key_name] = json.dumps(value)
                else:
                    result_dict[key_name] = convert_data(value)

        return result_dict


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

    @classmethod
    def de_parse_list(cls, prefix, json_list):
        '''
            de parse a list of data
        '''
        if isinstance(json_list, list):
            item_list = json_list
        else:
            item_list = json_list['users']

        results = ResultSet([cls.de_parse(prefix, obj) for obj in item_list])


class FollowRelations(Model):
    '''
        follow_relations class.
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
    def de_parse(cls, prefix, json):
        '''
            de parse
        '''
        result_dict = {}
        for key, value in json.iteritems():
            if key in {'_id', 'id', 'user_id'}:
                pass
            else:
                key_name = make_column_name(prefix, key)
                print key_name, value
                if key_name in USER_DATETIME_COLUMN_SET:
                    result_dict[key_name] = parse_datetime_into_hbase(value)
                elif key_name in USER_BOOLEAN_COLUMN_SET:
                    result_dict[key_name] = parse_boolean_into_hbase(value)
                elif key_name in USER_INT_COLUMN_SET:
                    result_dict[key_name] = parse_int_into_hbase(value)
                elif key_name in USER_LIST_COLUMN_SET:
                    json = import_simplejson()
                    result_dict[key_name] = json.dumps(value)
                else:
                    result_dict[key_name] = convert_data(value)

        return result_dict


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

    @classmethod
    def de_parse_list(cls, prefix, json_list):
        '''
            de parse a list of data
        '''
        if isinstance(json_list, list):
            item_list = json_list
        else:
            item_list = json_list['users']

        results = ResultSet([cls.de_parse(prefix, obj) for obj in item_list])


class ModelFactory(object):
    '''
        Used by parsers for creating instances of
        models. subclass this factary to add special
        models is also valid.
    '''
    user = User
    follow_relation = FollowRelations
