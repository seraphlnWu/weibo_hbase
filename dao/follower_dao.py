# coding=utf8

from datetime import datetime

from base import BaseQuery

from utils import MONGODB_INSTANCE
from utils import HBASE_INSTANCE

FOLLOWER_TB = HBASE_INSTANCE.table('followers')
FOLLOW_RELATION_TB = HBASE_INSTANCE.table('follow_relations')


class FollowerDao(BaseQuery):
    ''' inherit from base query '''
    #TODO, subclassing
    pass

class FollowRelationDao(BaseQuery):
    #TODO, subclassing
    pass


def get_follower_attr(uid, follower_id, attrs):
    """返回针对当前用户的评论数"""
    dao = FollowRelationDao()
    return dao.query_one(*attrs, id='%s_%s' % (uid, follower_id))
    
def get_cache_flwr_by_page(
    uid,
    sort_type='followers_count', 
    page=1, 
    records_per_page=10
):
    """
    TODO: wait for map reduce job
    """
    pass

def get_followers_by_page(uid, 
    sort_type='all',
    page=1, 
    records_per_page=10, 
    filterdict={}, 
):
    """
    TODO
    """
    
    records_per_page = [records_per_page, 1][records_per_page < 1]

    page_info = {
        'records_per_page': records_per_page,
        'sort_type': sort_type,
    }

    filter_items = {'user_id': uid}
    filter_items.update(filterdict)
    all_follow_relations = MONGODB_INSTANCE.follow_relations.find(filter_items)
    total_count = all_follow_relations.count()
    page_sum, rem = divmod(
        total_count, 
        records_per_page)
    page_sum += [1, 0][0 == rem]
    page = [page, 1][page < 1 or page > page_sum]
    fids = [
        f
        for f in all_follow_relations.skip(
            (page-1) * records_per_page
        ).limit(records_per_page)
    ]

    for fid in fids:
        ret = MONGODB_INSTANCE.followers.find_one({'_id': fid['follower_id']})
        if ret:
            fid.update({
                'name': ret['name'], 
                'profile_image_url': ret['profile_image_url'], 
                'verified': ret.get('verified'),
                'gender': ret.get('gender'),
                'location': ret.get('location'),
                'id': ret.get('_id'),
                'created_at_': ret.get('created_at'),
            })

    page_info.update({
        'page_totals': page_sum,
        'total_count': total_count,
        'current_page': int(page),
        'pre_page': page - 1 if page > 1 else page,
        'next_page': page + 1 if page < page_sum else page_sum,
    })
    return page_info, fids
    
def get_flwr_cache_up_time(uid):
    """
    TODO, wait for map reduce job
    """
    try:
        return MONGODB_INSTANCE.flwr_cache.find_one({'_id': uid}).get('upt')
    except:
        return datetime(2012, 01, 01)

def get_flwr_cache_tsts(uid):
    """
    TODO: wait for map reduce job
    """
    try:
        return MONGODB_INSTANCE.flwr_cache.find_one({'_id': uid}).get('tsts', 0)
    except:
        return 0

def get_flwr_actime_source(uid):
    return MONGODB_INSTANCE.flwr_actime_source.find_one({"_id":uid}) or {}


def get_follower_all(uid, keyword):
    """
    TODO
    """
    inf = map(
        lambda x: (
            x['follower_id'],
            x.get('friends_count', 0),
            x.get('followers_count', 0),
            x.get('statuses_count', 0),
            x.get('comment_count', 0),
            x.get('repost_count', 0),
            x.get('sm_flwr_quality', 0) * 100,
            x.get('activeness', 0) * 100,
        ),
        MONGODB_INSTANCE.follow_relations.find({'user_id': uid}).sort(keyword, -1).limit(50)
    )
    return inf
