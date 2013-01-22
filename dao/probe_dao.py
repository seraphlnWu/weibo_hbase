# coding=utf8

from utils import MONGODB_INSTANCE


def get_probes():
    ''' get whole probe list '''

    return [
        x['probe_uid']
        for x in MONGODB_INSTANCE.probes.find({}, {'probe_uid': 1})
    ]
