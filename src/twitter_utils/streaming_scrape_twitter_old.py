# -*- coding: utf-8 -*-
"""
Created on Wed Oct  6 13:17:00 2021

@author: icruicks
"""
import tweepy, time, os, logging
from datetime import datetime
from queue import Queue
from threading import Thread
from pymongo import MongoClient, DeleteOne

'''
Set up MongoDB to store collection and logging file
'''
client = MongoClient('foundation1.ece.local.cmu.edu', 27777)
db = client['military_vaccine']
collection = db['twitter']

logging.basicConfig(filename="Military_Vaccine_Twitter_Streaming_Logs.txt", filemode='a',
                    level=logging.INFO)
logger=logging.getLogger() 


'''
Define any functions or classes, and logging
'''
class MyStreamListener(tweepy.Stream):
    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret,
                 collection, q=Queue()):
        super(MyStreamListener, self).__init__(consumer_key, consumer_secret, access_token, access_token_secret)
        self.collection = collection
        self.i=0
        self.q = q
        for i in range(10):
            t = Thread(target=self.queue_tweets)
            t.daemon = True
            t.start()
        
    def on_status(self, tweet):
        self.q.put(tweet)
        
    def queue_tweets(self):
        while True:
            self.store_data(self.q.get())
            self.q.task_done()
            
    def store_data(self, tweet):
        dt_now =  datetime.now()
        tweet = tweet._json
        tweet['collection']= {
            'collection_time' : str(dt_now),
            'collected_by' : 'icruicks',
            }
        self.collection.insert_one(tweet)
        self.i +=1
        if self.i %10000 == 0:
            logging.info("{} Tweets processed".format(self.i))

    def on_exception(self, status):
        logging.exception("Exception occured: {}".format(status))
        return False
    
    def on_disconnect(self):
        logging.info("Disconnecting Listener, {} Tweets processed".format(self.i))


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
    
    logging.info("Removing duplicates. Total Number of Tweets Collected {}".format(collection.estimated_document_count()))


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
    access_token = f.read()
    
with open(os.path.join(key_dir, "afg_twitter_access_secret.txt"),'r') as f:
    access_token_secret= f.read()


'''
Specify any keywords or other search parameters
'''
track = ['military vaccine', 'military vaccine mandate']

'''
Collect the Tweets and store them in the MongoDB
'''
stream = MyStreamListener(consumer_key, consumer_secret, access_token, access_token_secret, collection)
for retry in range(2):
    try:
        stream.filter(track = track)
    except:
        logging.exception("Exception occured:")
        remove_duplicates(collection)

remove_duplicates(collection)

logging.info("/////////////////Final Collection Number////////////")
logging.info("Total Number of Tweets Collected {}".format(collection.estimated_document_count()))




