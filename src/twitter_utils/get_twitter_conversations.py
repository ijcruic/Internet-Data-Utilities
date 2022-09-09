# -*- coding: utf-8 -*-
"""
@author: icruicks
"""
import tweepy, time, os, logging, requests, base64, time, numpy as np
from datetime import datetime
from pymongo import MongoClient, DeleteOne

logging.basicConfig(filename="Mil_Vaccine_Conversations_Logs.txt", filemode='a',
                    level=logging.INFO)
logger=logging.getLogger()

'''
Set up the MongoDB
'''
client = MongoClient('localhost', 27777)
db = client['military_vaccine']
collection = db['twitter']


'''
Define any helper functions
'''
def get_bearer_header():
    uri_token_endpoint = 'https://api.twitter.com/oauth2/token'
    key_secret = f"{consumer_key}:{consumer_secret}".encode('ascii')
    b64_encoded_key = base64.b64encode(key_secret)
    b64_encoded_key = b64_encoded_key.decode('ascii')

    auth_headers = {
       'Authorization': 'Basic {}'.format(b64_encoded_key),
       'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
       }

    auth_data = {
       'grant_type': 'client_credentials'
       }

    auth_resp = requests.post(uri_token_endpoint, headers=auth_headers, data=auth_data)
    bearer_token = auth_resp.json()['access_token']

    bearer_header = {
       'Accept-Encoding': 'gzip',
       'Authorization': 'Bearer {}'.format(bearer_token),
       'oauth_consumer_key': consumer_key 
    }
    return bearer_header


def get_conversation_id(id):
    uri = 'https://api.twitter.com/2/tweets?'

    params = {
       'ids':id,
       'tweet.fields':'conversation_id'
    }
    try:
        bearer_header = get_bearer_header()
        resp = requests.get(uri, headers=bearer_header, params=params)
        conversation_id = resp.json()['data'][0]['conversation_id']
        return conversation_id
    except:
        return None
    
    
class ConversationScraper:
    def __init__(self,  api, collection):
        self.api = api
        self.collection = collection
        self.i = 0
    
    def get_conversations(self, conversation_ids):
        '''
        Get the tweets in conversations using v2 query to get the original tweet id,
        and then tweepy to get the full tweet
        '''
        for conversation_id  in conversation_ids:
            uri = 'https://api.twitter.com/2/tweets/search/all?'

            params = {'query': f'conversation_id:{conversation_id}',
                      "start_time": "2020-02-01T00:00:00.000Z",
                      "max_results": 500,
                      'tweet.fields': 'id',
            }
            try:
                bearer_header = get_bearer_header()
                
                for retry in range(2):
                    resp = requests.get(uri, headers=bearer_header, params=params)
                    if resp.json().get('title') == 'Too Many Requests':
                        time.sleep(60*15)
                        logging.info("Too many requests in period of time, sleeping for 15 minutes")
                    else:
                        break
                    
                if resp.json()['meta']['result_count'] > 0:
                    logging.info("Conversation ID {} had {} tweets in it".format(conversation_id, resp.json()['meta']['result_count']))
                    for partial_tweet in resp.json()['data']:
                        tweet_id = partial_tweet['id']
                        dt_now =  datetime.now()
                        tweet = api.lookup_statuses([tweet_id])[0]._json
                        tweet['collection']= {
                            'collection_time' : str(dt_now),
                            'collected_by' : 'icruicks',
                            }
                        tweet['conversation_id'] = conversation_id
                        self.collection.insert_one(tweet)
                        self.i +=1
                    
                    
                else:
                    logging.info(f"No conversation results for {conversation_id}")
                    continue
            except:
                logging.exception(f"Exception occured: for {conversation_id}")
                continue
                
        logging.info(f"Total tweet id's found in conversations: {self.i}")


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
    

'''
Get base data and add conversation_ids, if they exist, and then get the full tweets of 
any other tweets in the conversation
'''
i=0
conversation_ids =[]
collection.find({'conversation_id':{'$exists':False}}, no_cursor_timeout=True)
for tweet in cursor:
    conversation_id = get_conversation_id(tweet['id'])
    if conversation_id != None:
        collection.update_one({'_id':tweet['_id']}, {"$set":{"conversation_id":conversation_id}}, upsert=False)
        conversation_ids.append(conversation_id)
        i +=1
        if i %1000 == 0:
            logging.info("{} Tweets processed for conversation_ids".format(i))
cursor.close()

conversation_ids = list(set(conversation_ids)) #make sure to not duplicate any conversation_ids
logging.info("Total number of conversation IDs pulled {}".format(i))

convo_scraper = ConversationScraper(api, collection)
convo_scraper.get_conversations(conversation_ids)
                
                
remove_duplicates(collection)

logging.info("/////////////////Final Collection Number////////////")
logging.info("Total Number of Tweets Collected {}".format(collection.estimated_document_count()))

                                     