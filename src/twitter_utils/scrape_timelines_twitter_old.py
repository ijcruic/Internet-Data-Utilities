# -*- coding: utf-8 -*-
"""
Created on Mon Sep 20 15:17:15 2021

@author: icruicks
"""
import tweepy, time, os, logging
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, DeleteOne

'''
Define any functions
'''

def collect_timeline(user_name, collection, api):
    try:
        api.get_user(user_name)
    except tweepy.TweepError as e:
        logging.exception(user_name+" "+str(e))
        return
    else:
        i = 0
        for i in range(3):
            try:
                for tweet in tweepy.Cursor(api.user_timeline, screen_name=user_name, tweet_mode="extended").items():
                    dt_now =  datetime.now()
                    tweet = tweet._json
                    tweet['collection']= {
                        'collection_time' : str(dt_now),
                        'collected_by' : 'icruicks',
                        }
                    collection.insert_one(tweet)
                    i +=1
                    if i %100 == 0:
                        logging.info("{} Tweets processed".format(i))
            except:
                logging.exception("Exception occured for user: {}".format(user_name))
                time.sleep(15 * 60)


logging.basicConfig(filename="USAREC_Twitter_Scrape.txt", filemode='a',
                    level=logging.INFO)
logger=logging.getLogger() 

'''
Read in keys, set up files paths, and set up API
'''
# key_dir = os.path.join("C:", os.sep, "Users", "icruicks", "Documents", "Keys")
key_dir = "Keys"

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
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

'''
Set up MongoDB to store collection
'''
client = MongoClient('127.0.0.1', 2777)
db = client['USAREC']
collection = db['twitter']

'''
Read in files of seed handles
'''
user_names = pd.read_csv("usarec_twitter_accounts_first_hop.csv")

'''
Collect and store the Twitter data
'''
for user_name in user_names['USER NAMES'].tolist():
    collect_timeline(user_name, collection, api)
    
    
'''
Remove any duplicates
'''

pipeline = [
        {"$group": {"_id": "$id", "unique_ids": {"$addToSet": "$_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": { "$gte": 2 }}}
        ]

requests = []
for document in collection.aggregate(pipeline):
    it = iter(document["unique_ids"])
    next(it)
    for id in it:
        requests.append(DeleteOne({'_id': id}))
collection.bulk_write(requests)

logging.info("Total Number of Tweets Collected {}".format(collection.count_documents({})))

