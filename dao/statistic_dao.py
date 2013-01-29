# coding=utf8

from utils import MONGODB_INSTANCE
from utils import get_week_start
from utils import get_month_start
from utils import get_all_start

from influence_dao import get_cur_influence

from random import shuffle

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


def get_hotwords_graph(
    uid,
    total_count=30,
    search_from="mentions_hotwords",
    flag='only',
):
    ''' 获取一个账号的评论高频词列表 '''
    hotwords = MONGODB_INSTANCE[search_from].find_one({
        'sm_user_id': uid,
        'f_date': get_all_start(),
        'type': 'all',
    })

    result_list = getattr(hotwords, 'get', lambda x, y:[])('statistic', [])

    if result_list:
        tmp_result_list = [
            {
                "direction": cur_word, 
                "value": int(float(cur_count)/result_list[0][1] * 100),
                "description" : cur_count, 
            }
            for (cur_word, cur_count, cur_o, cur_r) in result_list[:total_count]
        ]
    else:
        tmp_result_list = []

    shuffle(tmp_result_list)
    if flag == 'only':
        return tmp_result_list
    else:
        result_dict = {'data': tmp_result_list}
        return result_dict


def get_hotwords_search(
    uid,
    h_type='all',
    total_count=10,
    search_from='buzz_hotwords',
):
    '''
        获取热词记录
        input:
            - uid: user id
            - h_type: week, month, all
            - total_count: 10, 25
            - search_from: buzz_hotwords, mention_hotwords,
                            directmsg_hotwords
        output:
            - [(, , ), (, , )...]
    '''
    search_dict = {
        'week': get_week_start, 
        'month': get_month_start,
        'all': get_all_start,
    }

    hotwords = MONGODB_INSTANCE[search_from].find_one({
        'sm_user_id': uid,
        'f_date': search_dict[h_type](),
        'type': h_type
    })

    try:
        result_list = hotwords.get('statistic', [])
    except AttributeError:
        result_list = []

    return result_list[:total_count]
