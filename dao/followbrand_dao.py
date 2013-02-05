# coding=utf8

import datetime
from weibo_dao.dao.base import BaseQuery
from weibo_dao.dao.utils import MONGODB_INSTANCE
from weibo_dao.dao.user_dao import get_fuids, get_user
from weibo_dao.dao.influence_dao import get_influence_by_date


class FollowbrandDao(BaseQuery):
    tb_name = 'followbrands'
    
def get_followbrands(uid, uidlist, sort_type='influence', sort_reverse=-1):
    '''
    input:
        - uid sina微博uid
    output: [{},...]
    ''' 
    fuids = get_fuids(uid)
    followbrand = []
    for fuid in fuids:
        if fuid in uidlist:
            influence_info = list(get_influence_by_date(
                fuid,
                sort_type,
                sort_reverse,
                limit=1,
            ))
            influence_info = influence_info[0] if len(influence_info) > 0 else {}
            user_info = get_user(fuid)

            influence_info['created_at'] = user_info.get("created_at", datetime.datetime(2011, 1,1))
            influence_info['url'] = user_info.get("url", "")
            influence_info['profile_image_url'] = user_info.get("profile_image_url", "")
            influence_info['description'] = user_info.get("description", "")
            influence_info['location'] = user_info.get("location", "")
            influence_info['screen_name'] = user_info.get("screen_name", "")
            influence_info['name'] = user_info.get("screen_name", "")
            influence_info['gender'] = user_info.get("gender", {})
            influence_info['verified'] = user_info.get("verified", {})
            influence_info['followbrand_id'] = user_info.get('id', 0)
            influence_info['fans_quality'] = user_info.get("fans_quality", 0)
            influence_info['sm_flwr_quality'] = user_info.get("fans_quality", 0)
            influence_info['fans_activeness'] = user_info.get("followers_activeness", 0)
            influence_info['activeness'] = user_info.get("account_activeness", 0)
        else:
            influence_info = list(get_followbrand_by_date(
                fuid,
                sort_type,
                sort_reverse,
                limit=1,
            ))
            influence_info = influence_info[0] if len(influence_info) > 0 else {}
        followbrand.append(influence_info)
    return followbrand


def get_followbrand_by_date(
    uid,
    sort_type='date',
    sort_reverse=True,
    limit=0,
):
    ''' get influence by limit '''
    if limit:
        return MONGODB_INSTANCE.followbrand.find({'followbrand_id': uid}).sort(sort_type, sort_reverse).limit(limit)
    else:
        return MONGODB_INSTANCE.followbrand.find({'followbrand_id': uid}).sort(sort_type, sort_reverse)