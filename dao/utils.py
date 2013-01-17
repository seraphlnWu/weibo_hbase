# coding=utf8

import pymongo
import happybase

from config import MONGODB_HOST
from config import MONGODB_PORT
from config import MONGODB_DBNAME


def get_db(
    mongo_host=MONGODB_PORT,
    mongo_port=MONGODB_PORT,
    mongo_dbname=MONGODB_DBNAME
):
    
    return pymongo.Connection(MONGODB_HOST, MONGODB_PORT)['sandbox_mongo_5']

MONGODB_INSTANCE = get_db()
