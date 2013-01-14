# coding=utf8
'''
    config file for parser
'''
USER_DATETIME_COLUMN_SET = {
    'user_tasks:flwr_update_time',
    'user_tasks:inf_update_time',
    'user_attrs:created_at',
    'user_attrs:join_at',
}

USER_BOOLEAN_COLUMN_SET = {
    'user_attrs:invalid',
    'user_attrs:sm_deleted',
    'user_attrs:verified',
    'user_tasks:is_probe',
    'user_attrs:fix_priority',
    'user_attrs:fixed',
    'user_attrs:vidt',
}

USER_INT_COLUMN_SET = {
    'user_attrs:gender',
    'user_tasks:mention_since_id',
    'user_tasks:comment_since_id',
    'user_api:exp',
    'user_tasks:priority',
    'user_tasks:day_last_mention_id',
    'user_tasks:latest_mention_id',
    'user_tasks:directmsg_since_id',
    'user_attrs:max_followbrand_count',
}

USER_LIST_COLUMN_SET = {
    'user_tasks:fuids',
    'user_tasks:task_list',
    'user_tasks:buzz_keywords',
    'user_tasks:mention_keywords',
    'user_tasks:direct_msg_tasks',
}

USER_API_COLUMN_FAMILY_SET = {
    'rt',
    'exp',
    'tok',
}

USER_ATTRS_COLUMN_FAMILY_SET = {
    'screen_name',
    'city',
    'created_at',
    'description',
    'join_at',
    'location',
    'url',
    'gender',
    'province',
    'profile_image_url',
    'invalid',
    'verified',
    'sm_deleted',
    'uw',
    'ins',
    'vidt',
    'max_followbrand_count',

    # what are these....
    'rpt',
    'ad',
    'fqua',
    'sct',
    'inf',
    'itr',
    'fct',
    'sts',
    'md',
    'ctf',
    'act',
    'cmt',
    'fact',
    'warn',
    'status',
    'friends_count',
}

USER_TASKS_COLUMN_FAMILY_SET = {
    'tasks',
    'fuids',
    'comment_since_id',
    'flwr_update_time',
    'mention_since_id',
    'day_last_mention_id',
    'latest_mention_id',
    'priority',
    'is_probe',
    'inf_update_time',
    'fix_priority',
    'fixed',
    'buzz_keywords',
    'directmsg_since_id',
    'mention_keywords',
    'direct_msg_tasks',
}

IGNORE_SET = {
    '_id',
    'id',
}
