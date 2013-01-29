# coding=utf8
from datetime import timedelta

from utils import MONGODB_INSTANCE
from utils import today_datetime
from smdata.utils import get_influence_list

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

def get_followers_activeness_distr(uid, reftime=None):
    '获取粉丝活跃度分布'
    return get_cur_influence(uid).get('followers_activeness_dist', {})

def get_fans_activeness_distr_data(uid, step_length=10):
    d = get_followers_activeness_distr(uid).items()
    d.sort(key=lambda x:int(x[0]))
    data = []
    dlen = len(d) / step_length
    num = 0

    sum_count = sum(map(lambda x: x[1], d))
    
    for n in range(dlen):
        s = sum(map(lambda x: x[1], d[n*step_length:][:step_length]))
        data.append("%s-%s;%0.1f" % (num, num+step_length, 100 * float(s) / max(1.0 , sum_count)))
        num += step_length 
    
    tmp_data = [x.split(';') for x in data]
    return '\\n'.join([
        ','.join([x for (x, y) in tmp_data]),
        ','.join([y for (x, y) in tmp_data])
    ])

def get_followers_quality_distr(uid, reftime=None):
    '获取粉丝质量度分布'
    return get_cur_influence(uid).get('followers_quality_dist', {})

def get_fans_activeness_data(uid):
    d = get_followers_activeness_distr(uid).items()
    d.sort(key=lambda x:int(x[0]))
    sum_count = sum(map(lambda x: x[1], d)) or 1

    title_list = ['不活跃(0-40)', '普通用户(40-60)', '活跃用户(60-75)', '高活跃用户(75-100)']
    val_list = [
        '%0.1f' % (sum(map(lambda x: x[1], d[:40])) * 100.0 / sum_count),
        '%0.1f' % (sum(map(lambda x: x[1], d[40:60])) * 100.0 / sum_count),
        '%0.1f' % (sum(map(lambda x: x[1], d[60:75])) * 100.0 / sum_count),
        '%0.1f' % (sum(map(lambda x: x[1], d[75:])) * 100.0 / sum_count),
    ]

    return '\\n'.join([
        ','.join(title_list),
        ','.join(val_list)
    ]) 


def get_fans_quality_distr(uid):
    ''' 获取粉丝的质量度分布 '''
    d = get_followers_quality_distr(uid).items()
    d.sort(key=lambda x: int(x[0]))
    sum_count = sum(map(lambda x: x[1], d)) or 1

    title_list = ['低质量用户(0-25)', '普通用户(25-50)', '高质量用户(50-75)', '骨灰用户(75-100)']
    val_list = [
        '%0.1f' % (sum(map(lambda x: x[1], d[:25])) * 100.0 / sum_count),
        '%0.1f' % (sum(map(lambda x: x[1], d[25:50])) * 100.0 / sum_count),
        '%0.1f' % (sum(map(lambda x: x[1], d[50:75])) * 100.0 / sum_count),
        '%0.1f' % (sum(map(lambda x: x[1], d[75:])) * 100.0 / sum_count),
    ]

    return '\\n'.join([
        ','.join(title_list),
        ','.join(val_list)
    ])

def get_histories_for_excel(uid, from_date=None, to_date=None):
    to_date = to_date or today_datetime()
    from_date = from_date or to_date - timedelta(days=30)
    histories = MONGODB_INSTANCE.influence.find(
        {
            'id': uid, 
            'date': {
                '$gte': from_date,
                '$lte': to_date
            }
        }).sort('date', -1)
        
    return get_influence_list(histories)

def get_influence_all(uid):
    to_date = today_datetime()
    from_date = to_date - timedelta(days=30)
    inf = map(
        lambda x: (
            x.get('date', from_date),
            x.get('influence', 0),
            x.get('account_activeness', 0) * 100,
            x.get('followers_activeness', 0) * 100,
            x.get('followers_quality', 0) * 100,
        ), get_histories_for_excel(uid)
    )
    return inf

def get_nflwr_all(uid, from_date, to_date):
    to_date = to_date or today_datetime()
    from_date = from_date or to_date - timedelta(days=6)
    inf = map(
        lambda x: (
            x.get('date', ''),
            x.get('nfcnt', 0)
        ), get_histories_for_excel(uid, from_date, to_date)
    )
    return inf 


def get_nctc_nrpc_all(uid, from_date, to_date):
    to_date = to_date or today_datetime()
    from_date = from_date or to_date - timedelta(days=6)
    inf = map(
        lambda x: (
            x.get('date', ''),
            x.get('nctc', 0),
            x.get('nrpc', 0)
        ), get_histories_for_excel(uid, from_date, to_date) 
    )
    return inf 

def get_flash_eyeball_all(uid, from_date, to_date):
    to_date = to_date or today_datetime()
    from_date = from_date or to_date - timedelta(days=6)
    inf = map(
        lambda x: (
            x.get('date', ''),
            x.get('sm_flash_factor', 0),
            x.get('sm_eyeball_factor', 0)
        ), get_histories_for_excel(uid, from_date, to_date)
    )
    return inf 

def get_flash_inf_all(uid, from_date, to_date):
    to_date = to_date or today_datetime()
    from_date = from_date or to_date - timedelta(days=6)
    inf = map(
        lambda x: (
            x.get('date', from_date + timedelta(days=1)),
            x.get('influence', 0),
            x.get('followers_count', 0),
            x.get('statuses_count', 0),
        ),get_histories_for_excel(uid, from_date, to_date) 
    )
    return inf

def get_histories_by_page(
    uid,
    sort_type='date',
    page=1,
    records_per_page=10,
    sort_reverse=True):

    histories = MONGODB_INSTANCE.influence.find(
        {'id': uid},
        {
            'date': 1,
            'influence': 1,
            'followers_count': 1,
            'statuses_count':1,
            'friends_count': 1,
            'account_activeness': 1,
            'followers_activeness': 1,
            'followers_quality': 1
        }
    )
    his_list = get_influence_list(histories) 
    #FIXME 没有考虑到可能没有某个关键词的情况
    his_list.sort(
        key=lambda x:x.get(sort_type, 0),
        reverse=[False, True][sort_reverse])

    page_info = {}
    if len(his_list) % records_per_page:
        page_info['page_totals'] = len(his_list) / records_per_page + 1
    else:
        page_info['page_totals'] = len(his_list) / records_per_page
        
    page = [page, 1][page < 1 or page > page_info['page_totals']]
    page_info['current_page'] = page
    page_info['records_per_page'] = records_per_page
    page_info['pre_page'] = page - 1 if page > 1 else page
    page_info['sort_type'] = sort_type
    if page < page_info['page_totals']:
        page_info['next_page'] = page + 1
    else:
        page_info['next_page'] = page_info['page_totals']

    return page_info, his_list[(page-1)*records_per_page: page*records_per_page]
    