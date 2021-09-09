# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 11:13:04 2021

@author: icruicks
"""
import tweepy, time, json, os, gzip
from datetime import datetime

'''
Define any helper functions
'''
def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            print('rate limit reached...resting')
            time.sleep(15 * 60)
        # except TweepError:
        #     print('rate limit reached...resting')
        #     time.sleep(15*60) 
        except StopIteration:
            print('reached end of queried tweets')
            break

'''
Read in keys, set up files paths, and set up API
'''
#key_dir = os.path.join("C:", os.sep, "Users", "icruicks", "Documents", "Keys")
key_dir = "Keys"
save_dir = os.path.join("/Storage3","Afghanistan")

with open(os.path.join(key_dir, "afg_twitter_consumer_key.txt"),'r') as f:
    consumer_key = f.read()
    
with open(os.path.join(key_dir, "afg_twitter_consumer_secret.txt"),'r') as f:
    consumer_secret= f.read()
    
with open(os.path.join(key_dir, "afg_twitter_access_key.txt"),'r') as f:
    access_token_key = f.read()
    
with open(os.path.join(key_dir, "afg_twitter_access_secret.txt"),'r') as f:
    access_token_secret= f.read()

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token_key, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

'''
Read in keywords and hashtags for the query terms, and any other arguments
'''
kwargs = {
"q" : '''afghanistan OR taliban OR kabul OR bagram OR "U.S. withdrawal" 
        OR afghan OR #afghanistan
        ''',
"start_date" : datetime(2021,8,8,0,0,0)
}

'''
Collect and store the Twitter data
'''
tweets = []
dt_now =  datetime.now()

try:
    for tweet in limit_handled(tweepy.Cursor(api.search, **kwargs).items()):
        tweet = tweet._json
        tweet['scrape_time'] = str(dt_now)
        tweet['scraped_by'] = 'icruicks'
        tweet['scrape_query'] = kwargs["q"]
        tweets.append(tweet)
except Exception as e:
    print("Exception occured:")
    print(e)
    
print("number of tweets scrapped: {}".format(len(tweets)))
    
    
with gzip.open(str(dt_now.year)+"_"+str(dt_now.month)+"_"+str(dt_now.day)+".json.gz", 'wt', encoding='utf-8') as f:
    json.dump(tweets, f, ensure_ascii=False)