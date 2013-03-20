# coding=utf8

from parser import ModelParser

from bson import ObjectId

from datetime import datetime


def testParseStatus():
    '''
        test parse a JSON object into HBase type.
    '''
    test_status_dict = {
        u'_id': 3532599710447824,
        u'atcnt': 0,
        u'bmiddle_pic': u'',
        u'created_at': datetime(2013, 1, 9, 16, 41, 6, 80000),
        u'favorited': False,
        u'geo': None,
        u'id': 3532599710447824,
        u'in_reply_to_screen_name': u'',
        u'in_reply_to_status_id': u'',
        u'in_reply_to_user_id': u'',
        u'original_pic': u'',
        u'retweeted_bmiddle_pic': u'',
        u'retweeted_original_pic': u'',
        u'retweeted_status': u'',
        u'retweeted_status_id': 0,
        u'retweeted_status_u_id': 0,
        u'retweeted_status_u_sname': u'',
        u'retweeted_thumbnail_pic': u'',
        u'sm_eyeball_factor': 34.137347835697994,
        u'sm_flash_factor': 149,
        u'source': u'\u65b0\u6d6a\u5fae\u535a',
        u'source_url': u'http://weibo.com/',
        u'text': u'\u5176\u5b9e\u88c513\u88c5\u9519\u5730\u65b9\u4e86\uff0c\u8fd8\u662f\u5f88\u5bb9\u6613\u88ab\u55b7\u7684\u3002\u3002\u3002',
        u'thumbnail_pic': u'',
        u'truncated': False,
        u'user_id': 1720690654,
    }

    model_parser = ModelParser()
    #import ipdb;ipdb.set_trace()
    result = model_parser.deserialized('status', test_status_dict)
    print result


def testParseFollowers():
    test_followers_info = {
        u'_id': 3119384225,
        u'activeness': 0.4080091791533603,
        u'avatar': u'http://tp2.sinaimg.cn/3119384225/180/5648352398/0',
        u'bfcnt': 65,
        u'city': u'1',
        u'created_at': datetime(2012, 11, 17, 16, 28, 32, 80000),
        u'description': u'2013\u5927\u6570\u636e\u8425\u9500\u56fd\u9645\u5cf0\u4f1a\u5c06\u4e8e5\u670831\u65e5\u5728\u5317\u4eac\u4e3e\u884c\uff0c\u8bf7\u7559\u610f\u6211\u4eec\u53d1\u5e03\u7684\u5927\u4f1a\u52a8\u6001\u3002',
        u'domain': u'bigdatamarketing',
        u'favourites_count': 0,
        u'fme': True,
        u'followers_count': 108,
        u'friends_count': 408,
        u'gender': u'f',
        u'id': 3119384225L,
        u'location': u'\u5317\u4eac \u4e1c\u57ce\u533a',
        u'name': u'\u5927\u6570\u636e\u8425\u9500\u5cf0\u4f1a',
        u'online': 0,
        u'profile_image_url': u'http://tp2.sinaimg.cn/3119384225/50/5648352398/0',
        u'province': u'11',
        u'screen_name': u'\u5927\u6570\u636e\u8425\u9500\u5cf0\u4f1a',
        u'sm_flwr_quality': 0.13789553131251653,
        u'sm_uids': [1720690654],
        u'sm_update_time': datetime(2013, 1, 9, 0, 0),
        u'status_created_at': datetime(2013, 1, 8, 18, 8, 26, 80000),
        u'statuses_count': 25,
        u'tags': [],
        u'url': u'',
        u'verified': False,
        u'vrson': u'',
    }
    model_parser = ModelParser()
    #import ipdb;ipdb.set_trace()
    result = model_parser.deserialized('followers', test_followers_info)
    print result


def testParseComments():
    test_comments_dict = {
        u'_id': 3532851142716342,
        u'buzz_keywords': [],
        u'created_at': datetime(2013, 1, 10, 9, 20, 12, 80000),
        u'ct': u'Wed Feb 01 11:42:10 +0800 2012',
        u'flw': True,
        u'ft': 815,
        u'gdr': u'm',
        u'id': 3532851142716342,
        u'loc': u'\u5c71\u4e1c',
        u'profile_image_url': u'http://tp4.sinaimg.cn/2608930403/50/5627963500/1',
        u'reply_comment_id': 0,
        u'sct': 1899,
        u'sft': 293,
        u'sm_user_id': 1667554942,
        u'source': u'<a href="http://app.weibo.com/t/feed/1sxHP2" rel="nofollow">\u4e13\u4e1a\u7248\u5fae\u535a</a>',
        u'status_id': 3532525345745887,
        u'text': u'[\u5a01\u6b66]',
        u'user_id': 2608930403,
        u'user_name': u'\u6da6\u534e\u51ef\u8fea\u62c9\u514b\u6dc4\u535a\u5468\u6751\u5e97',
        u'vfd': True
    }
    model_parser = ModelParser()
    #import ipdb;ipdb.set_trace()
    result = model_parser.deserialized('comments', test_comments_dict)
    print result


def testParseReposts():
    test_repost_dict = {
        u'_id': 3532865734604492,
        u'created_at': datetime(2013, 1, 10, 10, 18, 10, 80000),
        u'id': 3532865734604492,
        u're_cat': u'Thu Oct 27 13:49:07 +0800 2011',
        u're_flw': False,
        u're_flwrs_cnt': 253,
        u're_frds_cnt': 194,
        u're_gen': u'm',
        u're_loc': u'\u6d59\u6c5f \u5b81\u6ce2',
        u're_piurl': u'http://tp3.sinaimg.cn/2496865542/50/40005601753/1',
        u're_sts_cnt': 1874,
        u're_vfd': True,
        u'retweeted_status_id': 3532553044732249,
        u'screen_name': u'\u51ef\u8fea\u62c9\u514b\u5b81\u6ce2\u51ef\u8bda',
        u'sm_eyeball_factor': 62.66494008581362,
        u'sm_flash_factor': 253,
        u'sm_user_id': 1667554942,
        u'source': u'<a href="http://app.weibo.com/t/feed/1sxHP2" rel="nofollow">\u4e13\u4e1a\u7248\u5fae\u535a</a>',
        u'text': u'1954\u6b3eEldorado',
        u'user_id': 2496865542,
    }
    model_parser = ModelParser()
    #import ipdb;ipdb.set_trace()
    result = model_parser.deserialized('reposts', test_repost_dict)
    print result


def testParseFollowRelations():
    test_fr_dict = {
        u'_id': ObjectId('50ed4e52b1894c0f25aa3c9f'),
        u'activeness': 0.4080091791533603,
        u'created_at': datetime(2013, 1, 9, 19, 2, 40, 611000),
        u'follower_id': 3119384225L,
        u'followers_count': 108,
        u'friends_count': 408,
        u'sm_flwr_quality': 0.13789553131251653,
        u'statuses_count': 25,
        u'user_id': 1720690654,
        u'_id': 3119384225,
        u'activeness': 0.4080091791533603,
        u'avatar': u'http://tp2.sinaimg.cn/3119384225/180/5648352398/0',
        u'bfcnt': 65,
        u'city': u'1',
        u'created_at': datetime(2012, 11, 17, 16, 28, 32, 80000),
        u'description': u'2013\u5927\u6570\u636e\u8425\u9500\u56fd\u9645\u5cf0\u4f1a\u5c06\u4e8e5\u670831\u65e5\u5728\u5317\u4eac\u4e3e\u884c\uff0c\u8bf7\u7559\u610f\u6211\u4eec\u53d1\u5e03\u7684\u5927\u4f1a\u52a8\u6001\u3002',
        u'domain': u'bigdatamarketing',
        u'favourites_count': 0,
        u'fme': True,
        u'followers_count': 108,
        u'friends_count': 408,
        u'gender': u'f',
        u'id': 3119384225L,
        u'location': u'\u5317\u4eac \u4e1c\u57ce\u533a',
        u'name': u'\u5927\u6570\u636e\u8425\u9500\u5cf0\u4f1a',
        u'online': 0,
        u'profile_image_url': u'http://tp2.sinaimg.cn/3119384225/50/5648352398/0',
        u'province': u'11',
        u'screen_name': u'\u5927\u6570\u636e\u8425\u9500\u5cf0\u4f1a',
        u'sm_flwr_quality': 0.13789553131251653,
        u'sm_uids': [1720690654],
        u'sm_update_time': datetime(2013, 1, 9, 0, 0),
        u'status_created_at': datetime(2013, 1, 8, 18, 8, 26, 80000),
        u'statuses_count': 25,
        u'tags': [],
        u'url': u'',
        u'verified': False,
        u'vrson': u'',
    }
    model_parser = ModelParser()
    #import ipdb;ipdb.set_trace()
    result = model_parser.deserialized('follow_relation', test_fr_dict)
    print result


if __name__ == '__main__':
    print 'test status'
    testParseStatus()
    print 'test follower'
    testParseFollowers()
    print 'test comment'
    testParseComments()
    print 'test repost'
    testParseReposts()
    print 'test follow relations'
    testParseFollowRelations()
