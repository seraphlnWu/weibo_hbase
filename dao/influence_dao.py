# coding=utf8

from utils import MONGODB_INSTANCE
from utils import today_datetime


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
