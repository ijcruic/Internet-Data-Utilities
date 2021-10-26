# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 11:13:04 2021

@author: icruicks
"""
import tweepy, time, os, logging
from datetime import datetime
from pymongo import MongoClient, DeleteOne


logging.basicConfig(filename="Afghanistan_Twitter_Scrape_Logs.txt", filemode='a',
                    level=logging.INFO)
logger=logging.getLogger() 


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
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

'''
Set up MongoDB to store collection
'''
client = MongoClient('127.0.0.1', 2777)
db = client['afghanistan_withdrawal']
collection = db['twitter']

'''
Read in keywords and hashtags for the query terms, and any other arguments
'''
kwargs = {
"q" : '''afghanistan OR taliban OR kabul OR bagram OR "U.S. withdrawal" 
        OR afghan OR #afghanistan
        ''',
"start_date" : datetime(2021,9,13,0,0,0)
}

'''
Collect and store the Twitter data
'''
remove_duplicates(collection)

i = 0
for i in range(3):
    try:
        for tweet in tweepy.Cursor(api.search, **kwargs).items():
            dt_now =  datetime.now()
            tweet = tweet._json
            tweet['collection']= {
                'collection_time' : str(dt_now),
                'collected_by' : 'icruicks',
                'collected_query' : kwargs["q"]
                }
            collection.insert_one(tweet)
            i +=1
            if i %10000 == 0:
                logging.info("{} Tweets processed".format(i))
    
    except:
        logging.exception("Exception occured:")
        time.sleep(15 * 60)

remove_duplicates(collection)