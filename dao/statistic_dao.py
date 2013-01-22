# coding=utf8

from utils import MONGODB_INSTANCE

from influence_dao import get_cur_influence


def get_followers_location_distr(uid, reftime=None):
    '''获取粉丝地理分布'''
    return get_cur_influence(uid).get('followers_location_dist', {})


def get_followers_gender_distr(uid):
    '''获取账号性别分布'''
    return get_cur_influence(uid).get('followers_genders_dist', {})


def get_followers_quality_distr(uid, reftime=None):
    '''获取粉丝质量度分布'''
    return get_cur_influence(uid).get('followers_quality_dist', {})


def get_followers_tags_distr(uid, reftime=None):
    '''获取粉丝tags分布'''
    return get_cur_influence(uid).get('followers_tags_dist', {})


def get_celebrity_followers_quality_distr(uid):
    '''获取名人粉丝质量度分布'''
    result = []
    quality_distr = MONGODB_INSTANCE.celebrity.find_one({'celebrity_id':uid})

    if quality_distr:
        result = quality_distr.get("quality_ratio", {}).items()
    else:
        pass

    return result


def get_followbrand_followers_quality_distr(uid):
    '''获取竟品粉丝质量度分布'''
    result = []
    quality_distr = MONGODB_INSTANCE.followbrand.find_one({'followbrand_id':uid})
    if quality_distr:
        result = quality_distr.get("quality_ratio", {}).items()
    else:
        pass

    return result
