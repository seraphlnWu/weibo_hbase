# coding=utf8

from base import BaseQuery
from base import ResultList

from bson import ObjectId

from utils import MONGODB_INSTANCE

class UserDao(BaseQuery):
    ''' inherit from base query '''

    def __init__(self):
        ''' init func '''
        self.m_parser = ModelParser()
        self.table = HBASE_INSTANCE.table('status')
        self.db = MONGODB_INSTANCE

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


def save_sent_status(user_name, uid, pic_name, message, upt, cd=None):
    ''' save a status which need to be sent '''
    return MONGODB_INSTANCE.sent_status.insert(
        {
            'uname': user_name,
            'uid': uid,
            'pic_name': pic_name,
            'message': message,
            'upt': upt,
            'cd': cd,
        },
        safe=True,
    )


def update_sent_status_info(sts_id, upt):
    ''' update a to be sent status info '''
    MONGODB_INSTANCE.sent_status.update(
        {
            '_id': ObjectId(sts_id),
        },
        {
            '$set': {
                'sentdate': upt,
            }
        },
        safe=True
    )


def img_save(uid, pic_name, pic_content,content_type="image/png"):
    ''' 保存一张待发的图片 '''
    return MONGODB_INSTANCE.images.insert(
        {
            "uid": uid,
            "pic_name": pic_name,
            "pic_content": Binary(pic_content),
            "content_type": content_type, 
        },
        safe=True,
    )


def img_load(uid, pic_name):
    ''' 根据uid pic获取一张图片 '''
    imgs = MONGODB_INSTANCE.images.find_one({"uid": uid, "pic_name": pic_name})
    return imgs.get('pic_content'), imgs['content_type']


def img_remove(uid, pic_name):
    ''' remove a picture by uid and pic_name '''
    return MONGODB_INSTANCE.images.remove({"uid": uid, "pic_name": pic_name})
