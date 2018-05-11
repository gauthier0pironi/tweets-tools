# 
# Connects to twitter stream API for geo-localised tweets and dumps tweets in a db
#

from twitter import *
from datetime import datetime
import operator
import _mysql

#####
max_tweet_to_treat = 100000000
number_of_tweets_in_mysql_insert_batch = 10
#
db = _mysql.connect(host="127.0.0.1", user="python",
                  passwd="python", db="streaming")
#####
auth = OAuth(
    consumer_key='xxxxxx',
    consumer_secret='xxxx',
    token='xxxxxxxx',
    token_secret='xxxxxx'
)

tweet_treated = 0
tweet_to_insert = 0 
query = 'INSERT INTO streaming.tweets_geo VALUES'
twitter_sample_stream = TwitterStream(auth=auth, domain='stream.twitter.com')
for msg in twitter_sample_stream.statuses.filter( locations='-180,-90,180,90', language = 'en'): 
#for msg in twitter_sample_stream.statuses.sample( locations='-180,-90,180,90'): 
    try:
        if 'text' in msg: 
            if msg['geo'] is not None:
                tweet = msg['text'].encode('utf8')
                time_created = msg['created_at']
                d = datetime.strptime(time_created, '%a %b %d %H:%M:%S +0000 %Y');
                time_created = d.strftime('%Y-%m-%d %H:%M:%S')
                id = msg['id']
                tweet_treated += 1 
                geo_data = msg['geo']
                coordinates = geo_data['coordinates']
                coordinates_x = coordinates[0]
                coordinates_y = coordinates[1]
                print '.',
                tweet_to_insert += 1
                if tweet_to_insert == number_of_tweets_in_mysql_insert_batch:
                    query += '("' + str(id) + '","' + str(coordinates_x) +'","' + str(coordinates_y) +'","' + time_created + '",NOW(),"' + tweet.replace('"', '\\"') + '");'
                    db.query(query)
                    print tweet_treated, ' Tweets inserted in DB'
                    tweet_to_insert = 0
                    query = 'INSERT INTO streaming.tweets_geo VALUES'
                else:
                    query += '("' + str(id) + '","' + str(coordinates_x) +'","' + str(coordinates_y) +'","' + time_created + '",NOW(),"' + tweet.replace('"', '\\"') + '"),'
                if tweet_treated == max_tweet_to_treat:
                    break
            else:
                print 'X',
    except Exception as e:
        print 'Exception:', e, 'in query : ' , query
        query = 'INSERT INTO streaming.tweets_geo VALUES'
        tweet_to_insert = 0
        tweet_treated -= 20

# {u'favorited': False, u'contributors': None, u'truncated': False, u'text': u'Necesito comida', u'in_reply_to_status_id': None, u'user': {u'follow_request_sent': None, u'profile_use_background_image': True, u'default_profile_image': False, u'id': 2413652799, u'profile_background_image_url_https': u'https://abs.twimg.com/images/themes/theme1/bg.png', u'verified': False, u'profile_image_url_https': u'https://pbs.twimg.com/profile_images/444949889365966848/mkiyhcwG_normal.jpeg', u'profile_sidebar_fill_color': u'DDEEF6', u'profile_text_color': u'333333', u'followers_count': 126, u'profile_sidebar_border_color': u'C0DEED', u'id_str': u'2413652799', u'profile_background_color': u'C0DEED', u'listed_count': 1, u'is_translation_enabled': False, u'utc_offset': None, u'statuses_count': 2084, u'description': None, u'friends_count': 451, u'location': u'puerto rico', u'profile_link_color': u'0084B4', u'profile_image_url': u'http://pbs.twimg.com/profile_images/444949889365966848/mkiyhcwG_normal.jpeg', u'following': None, u'geo_enabled': False, u'profile_background_image_url': u'http://abs.twimg.com/images/themes/theme1/bg.png', u'name': u"Das'h", u'lang': u'es', u'profile_background_tile': False, u'favourites_count': 67, u'screen_name': u'anadashiira', u'notifications': None, u'url': None, u'created_at': u'Sat Mar 15 21:14:16 +0000 2014', u'contributors_enabled': False, u'time_zone': None, u'protected': False, u'default_profile': True, u'is_translator': False}, u'filter_level': u'medium', u'geo': None, u'id': 455505822622437376, u'favorite_count': 0, u'lang': u'es', u'entities': {u'symbols': [], u'user_mentions': [], u'hashtags': [], u'urls': []}, u'created_at': u'Mon Apr 14 00:40:25 +0000 2014', u'retweeted': False, u'coordinates': None, u'in_reply_to_user_id_str': None, u'source': u'<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>', u'in_reply_to_status_id_str': None, u'in_reply_to_screen_name': None, u'id_str': u'455505822622437376', u'place': None, u'retweet_count': 0, u'in_reply_to_user_id': None}
# {u'favorited': False, u'contributors': None, u'truncated': False, u'text': u'all I need in this life of sin, \nis me and my girlfriend \U0001f44c', u'in_reply_to_status_id': None, u'user': {u'follow_request_sent': None, u'profile_use_background_image': True, u'default_profile_image': False, u'id': 543614245, u'profile_background_image_url_https': u'https://pbs.twimg.com/profile_background_images/378800000160403596/amlFi5W9.jpeg', u'verified': False, u'profile_image_url_https': u'https://pbs.twimg.com/profile_images/454774286734327808/XHgCtPZ3_normal.jpeg', u'profile_sidebar_fill_color': u'DDEEF6', u'profile_text_color': u'333333', u'followers_count': 2298, u'profile_sidebar_border_color': u'FFFFFF', u'id_str': u'543614245', u'profile_background_color': u'FFFFFF', u'listed_count': 0, u'is_translation_enabled': False, u'utc_offset': -10800, u'statuses_count': 84975, u'description': u'grew up a fucking screw up. // @LOVE_MyPinkLips is da baby \u2665', u'friends_count': 1876, u'location': u'traphou$e.', u'profile_link_color': u'FA030B', u'profile_image_url': u'http://pbs.twimg.com/profile_images/454774286734327808/XHgCtPZ3_normal.jpeg', u'following': None, u'geo_enabled': True, u'profile_banner_url': u'https://pbs.twimg.com/profile_banners/543614245/1397035734', u'profile_background_image_url': u'http://pbs.twimg.com/profile_background_images/378800000160403596/amlFi5W9.jpeg', u'name': u'free Guwop.$$', u'lang': u'en', u'profile_background_tile': True, u'favourites_count': 4676, u'screen_name': u'iDoSlapHoes_', u'notifications': None, u'url': None, u'created_at': u'Mon Apr 02 18:10:06 +0000 2012', u'contributors_enabled': False, u'time_zone': u'Atlantic Time (Canada)', u'protected': False, u'default_profile': False, u'is_translator': False}, u'filter_level': u'medium', u'geo': None, u'id': 455505822630817793, u'favorite_count': 0, u'lang': u'en', u'entities': {u'symbols': [], u'user_mentions': [], u'hashtags': [], u'urls': []}, u'created_at': u'Mon Apr 14 00:40:25 +0000 2014', u'retweeted': False, u'coordinates': None, u'in_reply_to_user_id_str': None, u'source': u'web', u'in_reply_to_status_id_str': None, u'in_reply_to_screen_name': None, u'id_str': u'455505822630817793', u'place': None, u'retweet_count': 0, u'in_reply_to_user_id': None}
