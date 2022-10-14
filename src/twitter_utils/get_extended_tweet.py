# -*- coding: utf-8 -*-
"""
@author: icruicks
"""
import tweepy, time, os, logging, requests, base64, time, numpy as np
from datetime import datetime
from pymongo import MongoClient, DeleteOne

logging.basicConfig(filename="russia_ukraine_war_extend_Logs.txt", filemode='a',
                    level=logging.INFO)
logger=logging.getLogger()

'''
Set up the MongoDB
'''
client = MongoClient('foundation1.ece.local.cmu.edu', 27777)
db = client['russia_ukraine_war']
collection = db['twitter']


'''
Functions
'''

def remove_duplicates(collection):
    '''
    Remove any duplicates
    '''
    
    pipeline = [
            {"$group": {"_id": "$id", "unique_ids": {"$addToSet": "$_id"}, "count": {"$sum": 1}}},
            {"$match": {"count": { "$gte": 2 }}}
            ]
    
    requests = []
    for document in collection.aggregate(pipeline, allowDiskUse=True):
        it = iter(document["unique_ids"])
        next(it)
        for id_variable in it:
            requests.append(DeleteOne({'_id': id_variable}))
            
    if requests:
        collection.bulk_write(requests)
    
    logging.info("Total Number of Tweets Collected {}".format(collection.estimated_document_count()))


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
api = tweepy.API(auth, wait_on_rate_limit=True)


tweets = list(collection.find({"$and":[
    {'extended_tweet.full_text':{'$exists':False}}, {'full_text':{'$exists':False}}, {'retweeted_status':{'$exists':False}}
]}))

tweet_ids = [t['id'] for t in tweets]

tweet_ids_chunks = [tweet_ids [x:x+100] for x in range(0, len(tweet_ids), 100)]
i=0
for retry in range(100):
    try:
        for chunk in tweet_ids_chunks:
            for status in api.lookup_statuses(chunk, tweet_mode='extended'):
                tweet = status._json
                if 'extended_tweet' in tweet:
                    collection.update_one({'_id':tweet['id']}, {"$set":{"extended_tweet":tweet["extended_tweet"]}}, upsert=False)
                elif "full_text" in tweet:
                    collection.update_one({'_id':tweet['id']}, {"$set":{"full_text":tweet["full_text"]}}, upsert=False)
                i +=1
                if i %1000 == 0:
                    logging.info("{} Tweets processed for extended tweets".format(i))
                    
    except tweepy.errors.TooManyRequests:
        logging.exception("Exception occured: ")
        time.sleep(15* 60)
    
    except:
        logging.exception("Exception occured: ")
        time.sleep(5)
    
    
remove_duplicates(collection)

logging.info("/////////////////Final Collection Number////////////")
logging.info("Total Number of Tweets Collected {}".format(collection.estimated_document_count()))
    