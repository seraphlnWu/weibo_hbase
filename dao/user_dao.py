# coding=utf8

from utils import MONGODB_INSTANCE
from utils import HBASE_INSTANCE

from influence_dao import get_cur_influence

from weibo_dao.parser.parser import ModelParser

from social_master.smdata.models import init_user

USER_TABLE = HBASE_INSTANCE.table('users')

def get_users():
    ''' 获取全部的用户信息列表 '''
    mp = ModelParser()
    return [mp.parse('user', USER_TABLE.scan())]


def get_user_by_id(uid):
    ''' 根据传入的uid获取相应的user信息 '''
    mp = ModelParser()
    return mp.parse('user', USER_TABLE.row(str(uid)))

def get_user_by_keyword(uid, *keywords):
    ''' 根据传入的uid列表获取相应的user信息 '''
    cur_user = get_user_by_id(uid)
    return dict([
        (cur_key, cur_user.get(cur_key, None))
        for cur_key in keywords
    ])

def get_user_info(uid, default=['id', 'screen_name']):
    ''' 获取用户基本信息 '''
    cur_user = get_user_by_id(uid)
    return dict([
        (cur_key, cur_user.get(cur_key, None))
        for cur_key in default
    ])

def get_user(uid, user_data=None):
    '''
    获取用户基本信息，
    若库中没有用户记录，则按照new_user参数初始化用户基本信息。
    '''
    init_user(uid, user_data)

    resultdict = get_user_by_id(uid)

    if not resultdict:
        return {}

    influence = get_cur_influence(uid)
    resultdict['friends_count'] = influence.get('friends_count', 0)
    resultdict['followers_count'] = influence.get('followers_count', 0)
    resultdict['statuses_count'] = influence.get('statuses_count', 0)
    resultdict['influence'] = influence.get('influence', 0)
    resultdict['fans_quality'] = 0 if not resultdict['followers_count'] else resultdict['influence'] / resultdict['followers_count']
    resultdict['account_activeness'] = influence.get('account_activeness', 0)
    resultdict['followers_activeness'] = influence.get('followers_activeness', 0)
    resultdict['followers_quality_dist'] = influence.get('followers_quality_dist', {})
    resultdict['followers_location_dist'] = influence.get('followers_location_dist', {})
    resultdict['followers_genders_dist'] = influence.get('followers_genders_dist', {})
    resultdict['followers_activeness_dist'] = influence.get('followers_activeness_dist', {})
    resultdict['followers_tags_dist'] = influence.get('followers_tags_dist', {})

    if resultdict.get('url', '').strip() == 'http://1':
        resultdict['url'] = ''

    return resultdict
