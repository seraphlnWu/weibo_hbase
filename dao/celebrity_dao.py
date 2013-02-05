#coding:utf8

from re import compile as re_compile
from weibo_dao.dao.base import BaseQuery
from weibo_dao.dao.utils import MONGODB_INSTANCE
from smdata.utils import paginate
from smdata.utils import getLocationList
from smdata.utils import get_tags_distr
from smdata.utils import loc_str
from smdata.utils import convert_uid


class CelebrityDao(BaseQuery):
    tb_name = 'celebrity'

def search_celebrities_and_paginate(
    search_content='', 
    search_type ='', 
    sort_type='influence', 
    page=1, 
    records_per_page=10
):
    """
    TODO
    """

    if search_content.strip():
        celebrities = search_celebrities(search_content, search_type, sort_type)
    else:
        celebrities = search_celebrities(None, search_type, sort_type)

    page_info, celebrities = paginate(
        celebrities, 
        sort_type, 
        page, 
        records_per_page
    )
    
    for index, celebrity in  enumerate(celebrities):
        if celebrity.get("location_ratio", None):
            celebrities[index].update(
                {
                    "location_ratio": ''.join(map(
                        loc_str, 
                        getLocationList(celebrity["location_ratio"])
                    ))
                }
            )
        if celebrity.get("tag_ratio", None):
            celebrities[index].update(
                {
                    "tag_ratio": get_tags_distr(celebrity["tag_ratio"])
                }
            )
    return page_info, celebrities 


def search_celebrities_for_uid(search_items):
    """
    TODO
    """
    return set(
        [ celebrity['_id'] 
        for celebrity in MONGODB_INSTANCE.celebrity.find({
            '_id': {'$in': 
                [convert_uid(search_item.strip()) 
                    for search_item in search_items
                    if search_item.isdigit()
                ]}
            })
        ]
    )


def search_celebrities_for_screenname(search_items):
    """
    TODO
    """
    result_set_uids = set()
    name_results = []
    for search_item in search_items:
        name_results.extend([ 
            celebrity['_id'] 
            for celebrity in MONGODB_INSTANCE.celebrity.find({
                'screen_name': re_compile(r'.*%s.*' % search_item)
            })
        ])
    result_set_uids = result_set_uids.union(set(name_results))

    return result_set_uids


def search_celebrities_for_tags(search_items, search_type):
    """
    TODO
    """
    result_set_uids = set()
    tag_results = []
    if search_type == "and":
        tag_results.extend([
            celebrity['_id'] 
            for celebrity in MONGODB_INSTANCE.celebrity.find({
                'category_list': {'$all': search_items}
            })
        ])
        result_set_uids = result_set_uids.union(set(tag_results))
    else:
        resultlist = map(
            lambda t:[
                celebrity['_id'] 
                for celebrity in MONGODB_INSTANCE.celebrity.find({
                    'category_list': {'$all': [t]}
                })
            ], 
            search_items
        )
        for result in resultlist:
            result_set_uids = result_set_uids.union(set(result))

    return result_set_uids


def search_celebrities(search_content, search_type, sort_type='influence'):
    '''
    TODO
    根据查询条件，返回名人列表
    检索过程：
        - 检索uid结果
        - 模糊检索screen_name结果
        - 检索tag结果
        - 以上结果都是celebrity_id集合，最后根据celebrity_id集合获得最后检索结果
    '''
    if search_content:
        search_items = filter(lambda x: x, search_content.split(' '))
        result_set_uids = set()

        result_set_uids.update(search_celebrities_for_uid(search_items))

        result_set_uids.update(search_celebrities_for_screenname(search_items))

        result_set_uids.update(search_celebrities_for_tags(search_items, search_type))

        results = [celebrity for celebrity in MONGODB_INSTANCE.celebrity.find({
            '_id': {'$in': list(result_set_uids)}
        })]
    else:
        results = get_celebrities()

    results = filter(lambda x:x.get('created_at', None), results)
    results.sort(key=lambda x:x.get(sort_type, None), reverse=True)
    return results
        

def get_celebrities():
    '''
    TODO
    返回名人列表
    '''
    return [celebrity for celebrity in MONGODB_INSTANCE.celebrity.find({
         '_id': {
             '$nin': 
                    [
                        'sm_category_global_tags', 
                        'sm_category_global_tags_record'
                    ]
            }
        })]

def get_celebrity_categories():
    '''
    TODO
    返回名人类别列表，
    Note：名人类别列表，存在与celebrity中的'sm_category_global_tags'中
    '''
    cate_tags = MONGODB_INSTANCE.celebrity.find_one({'_id': 'sm_category_global_tags'})
    return [] if not cate_tags else cate_tags.get('cate_list', []) 


def add_celebrity_category_data(_id, name):
    """
    TODO
    """
    if _id in ['sm_category_global_tags', 'sm_category_global_tags_record']:
        cate_tags = MONGODB_INSTANCE.celebrity.find_one({'_id': _id})
        if cate_tags:
            category_list = cate_tags.get('cate_list', [])
            if name not in category_list:
                category_list.append(name)
                MONGODB_INSTANCE.celebrity.update(
                    {'_id': _id},
                    {'$set': {'cate_list': category_list}},
                    safe=True,
                )
            else:
                pass
        else:
            MONGODB_INSTANCE.celebrity.insert({
                '_id': _id,
                'cate_list': [name],
                },
                safe=True,
            )
    else:
        pass


def add_celebrity_category(name):
    '''
    TODO
    管理员添加名人分类类别
    NOTE：
        - sm_category_global_tags 用于分类过滤(添加，删除)
        - sm_category_global_tags_record 用于历史记录，为以后批量修改分类做准备(只添加，不删除)
    '''
    add_celebrity_category_data('sm_category_global_tags', name)
    add_celebrity_category_data('sm_category_global_tags_record', name)


def delete_filter_cate_by_cname(name):
    """
    TODO
    """
    cate_tags = MONGODB_INSTANCE.celebrity.find_one({
        '_id': 'sm_category_global_tags'
    })
    if cate_tags:
        category_list = cate_tags.get('cate_list', [])
        if name in category_list:
            category_list.remove(name)
            MONGODB_INSTANCE.celebrity.update(
                {'_id': 'sm_category_global_tags'},
                {'$set': {'cate_list': category_list}},
                safe=True,
            )
        else:
            pass
    else:
        pass

def add_celebrity_categories_by_str(cuid, cate_list_str):
    '''
    添加名人标签
    '''
    c = MONGODB_INSTANCE.celebrity.find_one({'_id': cuid})
    cate_list = cate_list_str.split(' ')
    if c:
        cate_set = set(cate_list)
        for cate in cate_set:
            add_celebrity_category_data('sm_category_global_tags_record', cate)
        cate_list = list(cate_set.union(set(c.get('category_list', []))))
        MONGODB_INSTANCE.celebrity.update(
            {'_id': cuid},
            {'$set': {'category_list': cate_list}},
            safe=True,
        )
        return MONGODB_INSTANCE.celebrity.find_one({'_id': cuid})
    else:
        return None

def delete_celebrity_cate(cuid, catename):
    '''
    删除名人标签
    '''
    c = MONGODB_INSTANCE.celebrity.find_one({'_id': cuid})
    cate_list = [catename]
    if c:
        cate_set = set(c.get('category_list', []))
        cate_list = list(cate_set.difference(set(cate_list)))
        MONGODB_INSTANCE.celebrity.update(
            {'_id': cuid},
            {'$set': {'category_list': cate_list}},
            safe=True,
        )
        return MONGODB_INSTANCE.celebrity.find_one({'_id': cuid})
    else:
        return None

def get_celebrity_followers_quality_distr(uid):
    '获取名人粉丝质量度分布'
    quality_distr = MONGODB_INSTANCE.celebrity.find_one({'celebrity_id':uid})
    if quality_distr:
        quality_distr = quality_distr.get("quality_ratio",{}).items()
    else:
        return []
    return quality_distr

def get_celebrity(celebrity_id):
    '''
    @todo: 根据名人ID返回名人
    @params celebrity_id: 名人id
    '''
    return  MONGODB_INSTANCE.celebrity.find_one({"celebrity_id": celebrity_id})
    