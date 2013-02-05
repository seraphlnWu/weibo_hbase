#coding:utf8

from weibo_dao.dao.utils import MONGODB_INSTANCE
from smdata.utils import paginate

def get_tasks_by_page(
    username, 
    uid, 
    sort_type='tctm', 
    page=1, 
    records_per_page=10
):
    tasks = MONGODB_INSTANCE.dtcenter.find(
        {
            'username': username, 
            "user_id": uid
        }
    )
    tasks = [x for x in tasks if not x.get('sm_deleted', False)]
    return paginate(
        tasks, 
        sort_type, 
        page, 
        records_per_page, 
    )