# coding=utf8
from utils import MONGODB_INSTANCE
from utils import today_datetime

from datetime import timedelta

from user_dao import get_keywords
from smdata.utils import paginate

def get_comments_by_page(
    uid, 
    page=1, 
    records_per_page=20, 
    filterable=False, 
    from_date=None, 
    to_date=None,
):
    ''' get the given uid's comments list '''
    today = today_datetime()
    keywords = get_keywords(uid)

    records_per_page = records_per_page if records_per_page > 1 else 1

    query_dict = {
        'sm_user_id': uid,
        'created_at': {
            '$gt': from_date or today - timedelta(days=6),
            '$lt': to_date or today + timedelta(days=1),
        },
    }
    if keywords and filterable:
        query_dict.update({'text': {'$regex': "|".join(keywords)}})
    else:
        pass

    page_info, comments = paginate(
        MONGODB_INSTANCE.comments.find(query_dict).limit(page*records_per_page),
        'created_at', 
        page,
        records_per_page,
    )

    return page_info, map(
        lambda x: (
            x['status_id'],
            x['sm_user_id'],
            x['text'],
            x.get('user_name', ''),  #FIXME, linkto  t.sina.com/<user_name> !!
            x['created_at'],
            x.get('profile_image_url',
                '/sm_media/img/default_thumbnail.gif'),
            x['user_id'],
            x['id']
        ),
        comments
    )
