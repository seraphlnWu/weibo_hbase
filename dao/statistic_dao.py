# coding=utf8

from utils import MONGODB_INSTANCE

from influence_dao import get_cur_influence

from datetime import datetime


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


def get_last_login_time_records(admaster_user):
    results = MONGODB_INSTANCE.login_time_records.find({
        "user_id":admaster_user.id
    }).sort("at", -1).limit(1)

    if results.count():
        return results[0]
    else:
        return None


def save_login_time_records(admaster_user):
    ''' 保存一条登陆记录 '''
    now = datetime.now()
    record = get_last_login_time_records(admaster_user)

    if any([
        not record,
        (now-record['at']).seconds >= 5*60,
    ]):
        MONGODB_INSTANCE.login_time_records.insert(
            {
                "user_id": admaster_user.id,
                 "name": admaster_user.username,
                 "at":now,
            },
            safe=True,
        )

