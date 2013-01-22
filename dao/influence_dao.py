# coding=utf8

from utils import MONGODB_INSTANCE
from utils import today_datetime

from datetime import timedelta


def get_cur_influence(uid):
    '获取用户可变属性。eg:影响力，粉丝数，微博数'
    inf_list = MONGODB_INSTANCE.influence.find(
        {'id': uid}
    ).sort('date', -1).limit(10)

    for cur_inf in inf_list:
        if any([
            cur_inf.get('followers_count'),
            cur_inf.get('influence'),
            cur_inf.get('followrs_activeness_distr'),
            cur_inf.get('friends_count'),
            cur_inf.get('statuses_count'),
        ]):
            return cur_inf

    return get_last_influence(uid)


def get_last_influence(uid):
    ''' 获取一条influence记录 '''
    return MONGODB_INSTANCE.influence.find_one({'id': uid}) or {}


def get_influence_history(uid, period=10, reftime=None):
    ''' 获取一个influence历史记录的列表 '''

    today = today_datetime()

    if reftime and reftime < today:
        pass
    else:
        reftime = today

    from_date = reftime - timedelta(period)

    result = MONGODB_INSTANCE.influence.find({
        'id': uid, 
        'date': {'$gt': from_date, '$lte': reftime},
    }).sort('date', -1)

    return check_influence_list(result)


def check_influence_list(histories):
    ''' 检查传入的influence列表中的数据是否合法 '''
    his_list = []
    for his in histories:
        if any([
            his.get('account_activeness', 0),
            his.get('followers_quality', 0),
            his.get('followers_activeness', 0)
        ]):
            if len(his_list) == 0:
                his_list.append(his)
            else:
                if not (his['date'].day - his_list[-1]['date'].day):
                    continue
                else:
                    his_list.append(his)
        else:
            pass

    return his_list


def get_followers_location_distr(uid, reftime=None):
    '获取粉丝地理分布'
    return get_cur_influence(uid).get('followers_location_dist', {})
