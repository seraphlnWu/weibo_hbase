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
from config import FOLLOWBRAND_FLWR_RELATIONS_COLUMN_DICT
from config import BUZZ_COLUMN_DICT


class ResultSet(list):
    '''A list like object that holds results from hbase'''


class Model(object):
    ''' base model '''
    columns_dct = {}
    reverse_column_dct = {}

    def __init__(self):
        ''' init the model '''

    @classmethod
    def serialized(self, json):
        ''' serialized the HBase type object into JSON type '''
        user = {}
        if isinstance(json, tuple):
            json = json[1]
        else:
            pass

        for key, value in json.iteritems():
            t_dct = self.reverse_column_dct.get(key)
            user[t_dct['column_name']] = PARSE_MAPPER[t_dct['type']](value)

        return user

    @classmethod
    def deserialized(self, json):
        ''' deserialized the JSON type object into a HBase type '''
        rst = {}
        for key, value in json.iteritems():
            t_dct = self.columns_dct.get(key)
            if t_dct:
                rst[t_dct['column_name']] = DEPARSE_MAPPER[t_dct['type']](value)
            else:
                pass

        return rst

    @classmethod
    def serialized_list(self, json_list):
        ''' serialized a list of hbase objects to json type. '''
        #return ResultSet([self.serialized(obj) for obj in json_list])
        for obj in json_list:
            yield self.serialized(obj)

    @classmethod
    def deserialized_list(self, json_list):
        ''' deserialized a list of JSON type objects to HBase type '''
        return ResultSet([self.deserialized(obj) for obj in json_list])

class Followers(Model):
    ''' followers class. '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        FOLLOWERS_COLUMN_DICT,
    )


class FollowRelations(Model):
    ''' follow_relations model.  '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        FOLLOW_RELATIONS_COLUMN_DICT,
    )


class FollowbrandFlwrRelations(Model):
    ''' followbrand flwr relations model. '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        FOLLOWBRAND_FLWR_RELATIONS_COLUMN_DICT,
    )


class FollowbrandFlwrs(Model):
    ''' followbrand flwrs model. '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        FOLLOWERS_COLUMN_DICT,
    )


class Comments(Model):
    ''' Comments class. '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        COMMENTS_COLUMN_DICT,
    )


class Reposts(Model):
    ''' Reposts class. '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        REPOSTS_COLUMN_DICT,
    )


class Mentions(Model):
    ''' Mention class. '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        MENTIONS_COLUMN_DICT,
    )


class MentionUsers(Model):
    ''' mention users class. '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        MENTION_USERS_COLUMN_DICT,
    )


class Status(Model):
    ''' status class '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        STATUS_COLUMN_DICT,
    )

class Buzz(Model):
    ''' buzz information class. '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        BUZZ_COLUMN_DICT,
    )

class BuzzData(Model):
    ''' new buzz information class. '''
    columns_dct, reverse_column_dct = reverse_the_column_to_key(
        BUZZ_COLUMN_DICT,
    )


class ModelFactory(object):
    '''
        Used by parsers for creating instances of
        models. subclass this factary to add special
        models is also valid.
    '''
    follow_relations = FollowRelations
    followers = Followers
    followbrand_flwr_relations = FollowbrandFlwrRelations
    followbrand_flwrs = FollowbrandFlwrs
    comments = Comments
    reposts = Reposts
    mentions = Mentions
    mention_users = MentionUsers
    status = Status
    buzz = Buzz
    buzz_data = BuzzData
