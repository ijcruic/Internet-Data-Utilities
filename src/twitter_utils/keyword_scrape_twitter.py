# -*- coding: utf-8 -*-
"""
Created on Wed Apr  12 11:13:04 2023

@author: icruicks
"""
import twint, time, os, logging, nest_asyncio, sys
from datetime import datetime
from pymongo import MongoClient, DeleteOne

nest_asyncio.apply()

'''
Specify any helper functions
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
        
    logging.info("{} Removing duplicates. Total Number of Tweets in Database {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S") , collection.estimated_document_count()))


'''
Specify data base to get conversations from
'''
db_name = sys.argv[1]
host = sys.argv[2]
search_keywords = sys.argv[3]
#db_name = 'killnet'
#host = 'foundation1.ece.local.cmu.edu'
repeats = 10

'''
Set envrionment
'''
client = MongoClient(host, 27777)
db = client[db_name]
collection = db["twitter"]


logging.basicConfig(filename=str(db_name)+"_Twitter_Scrape_Logs.txt", filemode='a',
                    level=logging.INFO)
logger=logging.getLogger() 

'''
Collect and Store Tweets
'''

logging.info("{} Total Number of Starting Tweet, Before Collection: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S") , collection.estimated_document_count()))
for i in range(repeats):
    if i >= 1:
        logging.info("{} Total Number of in Database {}, pausing collection".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S") , collection.estimated_document_count()))
        time.sleep(3600)

    # Collect the Tweets via Twint
    c = twint.Config()
    c.Store_object = True
    c.Search = search_keywords

    twint.run.Search(c)
    tweets = twint.output.tweets_list
    logging.info("{} Total Number of Tweets Collected {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S") , len(tweets)))

    # Store the collected tweets in a Mongo DB
    for raw_tweet in tweets:
        try:
            dt_now =  datetime.now()
            tweet = vars(raw_tweet)
            if 'tweet' in tweet.keys():
                tweet['text'] = tweet.pop('tweet')
            tweet['collection']= {
                'collection_time' : str(dt_now),
                'collected_by' : 'icruicks',
                'collected_query' : search_keywords
                }
            collection.insert_one(tweet)
        except:
            logging.exception("Exception occured: {}".format(tweet))

    remove_duplicates(collection)

remove_duplicates(collection)
logging.info("/////////////////Final Collection Number////////////")
logging.info("{} Total Number of Tweets in Database {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S") , collection.estimated_document_count()))