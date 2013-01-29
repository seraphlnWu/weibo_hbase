#coding=utf8

from datetime import datetime, timedelta

from utils import MONGODB_INSTANCE
from smdata.utils import convert_uid, paginate
from smdata.utils import call_pathmap

def get_repost_pathmap(status_id):
    ret = get_one_pathmap(status_id)
    current_time = datetime.now()
    check_time = current_time - timedelta(minutes=5)

    if ret is None or not ret.get("json_nfm"):

        MONGODB_INSTANCE.repost_pathmap.update(
            {"_id": status_id},
            {"$set": {"finished": 0}},
            upsert=True,
            safe=True)
        trans_result = call_pathmap(status_id)
        if trans_result is not None and "process_status" in trans_result:
            ret = get_one_pathmap(status_id)
        else:
            ret = { "timeline": None, "kol": None, "depth_map": None, "json_str": None, "json_nfm":None, }
       
 
    elif (ret["update_time"] - timedelta(days=2) <  ret["publish_time"]) and (check_time >  ret["update_time"]) :
        
        MONGODB_INSTANCE.repost_pathmap.update({"_id":status_id }, {"$set": {"finished":0},}, safe=True )
        trans_result = call_pathmap(status_id)
        if trans_result is not None and "process_status" in trans_result:
            ret = get_one_pathmap(status_id)

    return ret["timeline"], ret["kol"], ret["depth_map"], ret["json_str"], ret["json_nfm"]

def get_one_pathmap(status_id):
    ret_cursor = MONGODB_INSTANCE.repost_pathmap.find( 
                                         { "_id":status_id,"finished":1 },
                                         { "update_time":1,"publish_time":1,"timeline":1,"kol":1,"depth_map":1, "json_str":1, "json_nfm":1, }

                                       ).sort("update_time",-1).limit(1)
    ret = None
    for item in ret_cursor:
        ret = item
        break
    return ret



def get_repost_data(
    uid, 
    status_id, 
    sort_type='cnt', 
    page=1, 
    records_per_page=10, 
    sort_reverse=True
):
    timeline, tmp_kol, depth_map, repost_pathmap, json_nfm = get_repost_pathmap(convert_uid(status_id))
    for x in depth_map:
        x['cntt'] = x.get('cnt', 0) + x.get('cnt:', 0)

    tmp_kol = tmp_kol[1:]
    page_info, kol = paginate(
        tmp_kol, 
        sort_type, 
        page, 
        records_per_page, 
        sort_reverse
    )
    return page_info, kol, depth_map, repost_pathmap
    