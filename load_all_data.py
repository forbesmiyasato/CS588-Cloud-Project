import json
import redis
import argparse
import time
from utils import get_logger
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('--file', required=True, type=str)
args = parser.parse_args()
file = args.file

LOGGER = get_logger('load tweet data')

r = redis.Redis(host='localhost', port=6379, db=0)

def main():
    with open(file) as f:
        tweets = json.loads(f.read())

    start_time = time.perf_counter()
    load_tweet_data(tweets)
    load_user_data(tweets)
    load_hashtag_data(tweets)
    load_country_data(tweets)
    total_time = time.perf_counter() - start_time
    LOGGER.info(f"Loading all data took {total_time:0.2f} seconds to run")

def load_tweet_data(tweets):
    LOGGER.info('Starting to load tweet data')
    start_time = time.perf_counter()

    for tweet in tweets:
        if 'id' not in tweet:
            continue
        tweet_id = tweet['id']
        created_time = tweet['created_at']
        text = tweet['text']
        user_key = tweet['user']['id']
        tweet_type = 'Simple'
        if tweet['in_reply_to_status_id']:
            tweet_type = 'reply'
        elif 'retweeted_status' in tweet:
            if tweet['retweeted_status']['is_quote_status']:
                tweet_type = 'retweet with quote'
            else:
                tweet_type = 'retweet'
        reply_to_user_key = 'None'
        if tweet['in_reply_to_user_id']:
            reply_to_user_key = tweet['in_reply_to_user_id']
        reply_to_tweet_id = 'None'
        if tweet['in_reply_to_status_id']:
            reply_to_tweet_id = tweet['in_reply_to_status_id']
        created_dt = datetime.strptime(created_time,'%a %b %d %H:%M:%S +0000 %Y') 

        r.sadd('all_tweets', tweet_id)
        r.hmset(f'tweet:{tweet_id}', {'tweet_id': tweet_id, 'type': tweet_type,
                                'created_time': created_time, 'text': text,
                                'user_key': user_key, 'reply_to_user_key': reply_to_user_key})
        r.zadd(f'reply_tweets:{reply_to_tweet_id}', {tweet_id: created_dt.timestamp()}, nx=True)

    total_time = time.perf_counter() - start_time
    LOGGER.info(f"Loading tweet data took {total_time:0.2f} seconds to run")


def load_user_data(tweets):
    LOGGER.info('Starting to load user data')
    start_time = time.perf_counter()

    for tweet in tweets:
        if 'user' not in tweet:
            continue
        id = tweet['user']['id']
        username = tweet['user']['name']
        screen_name = tweet['user']['screen_name']
        r.hset('all_users', username, id)
        r.hmset(f'user:{id}', {'username': username, 'screen_name': screen_name})
        created_time = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
        r.zadd(f'user_tweets:{id}', {tweet['id']: created_time.timestamp()}, nx=True)

    total_time = time.perf_counter() - start_time
    LOGGER.info(f"Loading user data took {total_time:0.2f} seconds to run")


def load_hashtag_data(tweets):
    LOGGER.info('Starting to load hashtag data')
    start_time = time.perf_counter()

    for tweet in tweets:
        if ('id' not in tweet or 'entities' not in tweet 
            or 'hashtags' not in tweet['entities']):
            continue
        hashtags = []
        for hashtag in tweet['entities']['hashtags']:
            hashtags.append(hashtag['text'])
        if hashtags:
            r.sadd('all_hashtags', *hashtags)
        created_time = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
        for hashtag in hashtags:
            r.zadd(f'hashtag_tweets:{hashtag}', {tweet['id']: created_time.timestamp()},
                nx=True)

    total_time = time.perf_counter() - start_time
    LOGGER.info(f"Loading hashtag data took {total_time:0.2f} seconds to run")


def load_country_data(tweets):
    LOGGER.info('Starting to load country data')
    start_time = time.perf_counter()

    country_dictionary = {}
    for tweet in tweets:
        if 'place' not in tweet:
            continue

        if tweet['place'] == None:
            continue

        if 'country' not in tweet['place']:
            continue

        if tweet['place']['country'] == None:
            continue

        country = tweet['place']['country']

        if country in country_dictionary:
            country_dictionary[country] += 1
        else:
            country_dictionary[country] = 1

    for key, value in country_dictionary.items():
        r.zadd("Country",{key: value}, incr=True)

    total_time = time.perf_counter() - start_time
    LOGGER.info(f"Loading country data took {total_time:0.2f} seconds to run")


if __name__ == "__main__":
    main()
