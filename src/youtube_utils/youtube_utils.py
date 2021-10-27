# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 11:13:05 2021

@author: icruicks
"""
import requests, pafy, os, logging
import concurrent.futures
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi as yapi
from urllib.parse import urlparse, parse_qs

'''
Set global variables and important lists of stuff like links to unshorten, or
those to keep query terms on, etc.
'''

MAX_THREADS = 20


'''
Define module-level functions and classes
'''

class get_youtube_data_from_urls:
    
    def __init__(self, api_key=None, save_full_video=False, 
                 video_save_dir="youtube_videos", save_img_thumbnail=False,
                 img_save_dir="youtube_thumbnails"):
        
        self.api_key = api_key
        self.video_save_dir = video_save_dir
        self.img_save_dir = img_save_dir
        self.save_full_video = save_full_video
        self.save_img_thumbnail = save_img_thumbnail
        
        #create video directory, if needed
        if save_full_video:
            if video_save_dir == "youtube_videos":
                if video_save_dir not in os.listdir():
                    try:
                        os.mkdir(video_save_dir)
                    except OSError:
                        pass
                
        #create image directory, if needed
        if save_img_thumbnail:
            if img_save_dir == "youtube_thumbnails":
                if img_save_dir not in os.listdir():
                    try:
                        os.mkdir(img_save_dir)
                    except OSError:
                        pass
                
        # Set the API key for PAfy
        if api_key != None:
            pafy.set_api_key(api_key)
                
        
    def retrieve_from_url(self, url):
        
        if self.save_full_video: 
            videos_already_downloaded = [i[:-4] for  i in os.listdir(self.video_save_dir)]
        else:
            videos_already_downloaded = []
        
        datum ={'url': url,
                'video_id': 'None',
                'transcript': 'None',
                'category': 'None',
                'likes': 'None',
                'dislikes': 'None',
                'length': 'None',
                'rating': 'None',
                'viewcount': 'None',
                'title': 'None',
                'description': 'None',
                'keywords': 'None',
                'author': 'None'}
        
        if url !='None':
            
            # Get the Youtube video ID and check that it is a Youtube URL
            query = urlparse(url)
            if query.hostname == 'youtu.be':
                video_id = query.path[1:]
            elif '/embed/' in query.path:
                video_id = query.path.split('/')[-1]
            elif query.hostname in ('www.youtube.com', 'youtube.com', 'm.youtube.com'):
                p = parse_qs(query.query)
                video_id = p.get('v',['None'])[0]
            else:
                return datum
            
            datum['video_id'] = video_id
                
            #Pull the metadata for the YouTube film
            try:
                video_stats = pafy.new(url)
                try:
                    datum['category'] = video_stats.category
                    datum['likes'] = video_stats.likes
                    datum['dislikes'] = video_stats.dislikes
                    datum['length'] = video_stats.length
                    datum['rating'] = video_stats.rating
                    datum['viewcount'] = video_stats.viewcount
                    datum['title'] = video_stats.title
                    datum['author'] = video_stats.author
                except:
                    logging.error(url+" : unable to get any video stats")
                
                try:
                    datum['description'] = video_stats.description
                    datum['keywords'] = video_stats.keywords
                except:
                    logging.error(url+" : unable to get video stats that require a API key")
                
                # Save out the video thumbnail image, if desired
                if self.save_img_thumbnail:
                    try:
                        thumb_url = video_stats.thumb
                        with open(os.path.join(self.img_save_dir, video_id+thumb_url[-4:]), 'wb') as f:
                            response = requests.get(thumb_url)
                            f.write(response.content)
                    except:
                        logging.error("Unable to access thumbnail image: "+thumb_url)
                
                # Save out the full video, if desired
                if self.save_full_video:
                    if video_id not in videos_already_downloaded:
                        try:
                            stream = video_stats.getbest()
                            stream.download(filepath=os.path.join(self.video_save_dir,video_id+".mp4"))
                            
                        except:
                            logging.error("Unable to get video content"+url)
                
            except:
                logging.error(url+" : unable to access video via pafy")
                  
            '''
            Pull the transcript from YouTube, if available
            '''
            try:
                trans_list = yapi.list_transcripts(video_id)
            except:
                logging.error(url+" : unable to get video transcript")
                datum['transcript'] = 'no transcript'
            else:
                try:
                    transcript = trans_list.find_transcript(['en']).fetch()
                except:
                    for trans in trans_list:
                        if trans.is_translatable:
                            transcript = trans.translate('en').fetch()
                            break
                        
                text_list = [entry['text'] for entry in transcript]
                datum['transcript'] = " ".join(text_list).strip()
            
            return datum
        
        
    def retreive_from_urls(self, list_of_urls):
        threads = min(MAX_THREADS, len(list_of_urls))
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            processed_urls =executor.map(self.retrieve_from_url, list_of_urls)
            
        return pd.DataFrame(processed_urls)