# coding=utf8

from utils import MONGODB_INSTANCE

def get_benchmark_by_keyword(ins_key):
    ''' get benchmark by given keyword '''
    return MONGODB_INSTANCE.benchmark.find_one({'_id': ins_key})
