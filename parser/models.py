# coding=utf8
'''
    model factary.

    In this model I defined all the models those
    needed to be translate from MongoDB type to HBase type or
    from HBase type to MongoDB.

    The ModelFactory class collect all the models that
    can be used.
'''
from utils import PARSE_MAPPER
from utils import DEPARSE_MAPPER

from utils import reverse_the_column_to_key

from config import FOLLOW_RELATIONS_COLUMN_DICT
from config import FOLLOWERS_COLUMN_DICT
from config import COMMENTS_COLUMN_DICT
from config import REPOSTS_COLUMN_DICT
from config import MENTIONS_COLUMN_DICT
from config import MENTION_USERS_COLUMN_DICT
from config import STATUS_COLUMN_DICT


class ResultSet(list):
    '''A list like object that holds results from hbase'''


class Model(object):
    ''' base model '''
    columns_dct = {}
    reverse_column_dct = {}
    

    def __init__(self):
        ''' init the model '''

    @classmethod
    def serialized(cls, json):
        ''' serialized the HBase type object into JSON type '''
        user = {}
        for key, value in json.iteritems():
            t_dct = cls.reverse_column_dct.get(key)
            user[t_dct['column_name']] = PARSE_MAPPER[t_dct['type']](value)
        return user

    @classmethod
    def deserialized(cls, json):
        ''' deserialized the JSON type object into a HBase type '''
        rst = {}
        for key, value in json.iteritems():
            t_dct = cls.columns_dct.get(key)
            if t_dct:
                rst[t_dct['column_name']] = DEPARSE_MAPPER[t_dct['type']](value)
            else:
                print key
        return rst

    @classmethod
    def serialized_list(cls, json_list):
        ''' serialized a list of hbase objects to json type. '''
        return ResultSet([cls.parse(obj) for obj in json_list])

    @classmethod
    def deserialized_list(cls, json_list):
        ''' deserialized a list of JSON type objects to HBase type '''
        return ResultSet([cls.de_parse(obj) for obj in json_list])


class FollowRelations(Model):
    ''' follow_relations model.  '''

    def __init__(cls):
        cls.columns_dct, cls.reverse_column_dct = reverse_the_column_to_key(
            FOLLOW_RELATIONS_COLUMN_DICT,
        )


class Followers(Model):
    ''' followers class. '''
    def __init__(cls):
        cls.columns_dct, cls.reverse_column_dct = reverse_the_column_to_key(
            FOLLOWERS_COLUMN_DICT,
        )


class Comments(Model):
    ''' Comments class. '''
    def __init__(cls):
        cls.columns_dct, cls.reverse_column_dct = reverse_the_column_to_key(
            COMMENTS_COLUMN_DICT,
        )


class Reposts(Model):
    ''' Reposts class. '''
    def __init__(cls):
        cls.columns_dct, cls.reverse_column_dct = reverse_the_column_to_key(
            REPOSTS_COLUMN_DICT,
        )


class Mentions(Model):
    ''' Mention class. '''
    def __init__(cls):
        cls.columns_dct, cls.reverse_column_dct = reverse_the_column_to_key(
            MENTIONS_COLUMN_DICT,
        )


class MentionUsers(Model):
    ''' mention users class. '''
    def __init__(cls):
        cls.columns_dct, cls.reverse_column_dct = reverse_the_column_to_key(
            MENTION_USERS_COLUMN_DICT,
        )


class Status(Model):
    ''' status class '''
    def __init__(cls):
        cls.columns_dct, cls.reverse_column_dct = reverse_the_column_to_key(
            STATUS_COLUMN_DICT,
        )


class ModelFactory(object):
    '''
        Used by parsers for creating instances of
        models. subclass this factary to add special
        models is also valid.
    '''
    follow_relation = FollowRelations
    followers = Followers
    comments = Comments
    reposts = Reposts
    mentions = Mentions
    mention_user = MentionUsers
    status = Status
