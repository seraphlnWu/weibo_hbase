# coding=utf8

from datetime import datetime

from base import BaseQuery
from base import ResultList

from utils import MONGODB_INSTANCE
from utils import HBASE_INSTANCE

from utils import compare_value

from influence_dao import get_cur_influence

from weibo_dao.parser.parser import ModelParser

from weibo_dao.parser.utils import make_column_name


class UserDao(BaseQuery):
    ''' inherit from base query '''

    def __init__(self):
        ''' init func '''
        self.m_parser = ModelParser()
        self.table = HBASE_INSTANCE.table('users')

    def query(self, *args, **kwargs):
        ''' query users '''
        return [self.m_parser('user', self.table.scan())]

    def query_one(self, *args, **kwargs):
        ''' query one user '''
        column_list = []
        if args:
            column_list = [make_column_name('users', attr) for attr in args]
        else:
            pass

        return self.m_parser.parse(
            'user',
            self.table.row(str(kwargs.get('id')), columns=column_list),
        )

    def put_one(self, *args, **kwargs):
        ''' put / update one user '''
        row_key = kwargs.pop('id')
        self.table.put(str(row_key), self.m_parser.de_parse(kwargs))

    def delete(self, *args, **kwargs):
        ''' delete records '''
        pass


def get_users():
    ''' 获取全部的用户信息列表 '''
    user_dao = UserDao()
    return user_dao.query()


def get_user_by_id(uid):
    ''' 根据传入的uid获取相应的user信息 '''
    user_dao = UserDao()
    return user_dao.query_one(**{'id': uid})


def get_user_by_keyword(uid, *keywords):
    ''' 根据传入的uid列表获取相应的user信息 '''
    user_dao = UserDao()
    return user_dao.query_one(*keywords, **{'id': uid})


def get_user_info(uid, default=['id', 'screen_name']):
    ''' 获取用户基本信息 '''
    user_dao = UserDao()
    return user_dao.query_one(*default, **{'id': uid})


def get_tasks(uid):
    ''' get a task list by uid '''
    user_dao = UserDao()
    return user_dao.query_one(*['tasks'], **{'id': uid}) or []


def get_user(uid):
    '''
        获取用户基本信息，
        若库中没有用户记录，则按照
        new_user参数初始化用户基本信息。
    '''
    user_dao = UserDao()
    resultdict = user_dao.query_one(**{"id": uid})

    if not resultdict:
        return {}

    influence = get_cur_influence(uid)
    resultdict['friends_count'] = influence.get('friends_count', 0)
    resultdict['followers_count'] = influence.get('followers_count', 0)
    resultdict['statuses_count'] = influence.get('statuses_count', 0)
    resultdict['influence'] = influence.get('influence', 0)
    resultdict['fans_quality'] = resultdict['influence']/resultdict['followers_count'] if resultdict['followers_count'] else 0
    resultdict['account_activeness'] = influence.get('account_activeness', 0)
    resultdict['followers_activeness'] = influence.get('followers_activeness', 0)
    resultdict['followers_quality_dist'] = influence.get('followers_quality_dist', {})
    resultdict['followers_location_dist'] = influence.get('followers_location_dist', {})
    resultdict['followers_genders_dist'] = influence.get('followers_genders_dist', {})
    resultdict['followers_activeness_dist'] = influence.get('followers_activeness_dist', {})
    resultdict['followers_tags_dist'] = influence.get('followers_tags_dist', {})

    if resultdict.get('url', '').strip() == 'http://1':
        resultdict['url'] = ''

    return resultdict


def add_task(uid, task):
    ''' 添加一个新的待发送的微博 '''
    task_list = get_tasks(uid)
    task_list.append(task)
    task_list.sort(key=lambda x:x.get('eta'))

    task_length = len(task_list)
    cur_index = task_list.index(task)
    k = cur_index + 1  # if cur_index < len(task_list)-1 else cur_index
    j = cur_index - 1  # if cur_index > 0 else 0

    if any([
        (j >= 0) and (task['message'] == task_list[j].get("message")),
        k < task_length and (task['message'] == task_list[k]["message"]),
        (j >= 0 and (task['eta'] - task_list[j].get('eta')) < 60),
        (k < task_length and (task['eta'] - task_list[k]['eta']) < 60),
    ]):
        return False
    else:
        user_dao = UserDao()
        user_dao.put_one(**{'id': uid, 'tasks': task_list, })

        return True


def del_task(uid, task_timestamp, flag='tid'):
    ''' 删除一个待发送的微博 '''
    task_list = get_tasks(uid)

    for i, cur_task in enumerate(task_list):
        if cur_task.get(flag) == task_timestamp:
            del task_list[i]
            break
    else:
        return True

    user_dao = UserDao()
    user_dao.put_one(**{'id': uid, 'tasks': task_list, })

    return True


def get_buzz_keywords(uid):
    ''' get target uid's buzz keywords '''
    user_dao = UserDao()
    return user_dao.query_one(*['buzz_keywords'], **{'id': uid})


def set_buzz_keywords(uid, keywords=[]):
    """ set buzz_keywords """
    flag = False
    usr = get_user_by_id(uid)
    if not all((usr, isinstance(keywords, list))):
        pass
    else:
        user_dao = UserDao()
        user_dao.put_one(**{'id': uid, 'buzz_keywords': keywords}) 
        flag = True    

    return flag


def del_buzz_keyword(uid, keywords=[]):
    """ delete buzz_keywords """
    flag = False
    usr = get_user_by_id(uid)
    if not all((usr, isinstance(keywords, list))):
        pass
    else:
        user_dao = UserDao()
        user_dao.put_one(**{'id': uid, 'buzz_keywords': keywords}) 
        flag = True    

    return flag
