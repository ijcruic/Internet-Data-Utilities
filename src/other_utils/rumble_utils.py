# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 12:05:25 2021

@author: icruicks
"""
import requests, urllib3, pafy, re, os, logging, time, random
import concurrent.futures
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from bs4 import BeautifulSoup 
from urllib.parse import urljoin, urlunparse, urlparse, parse_qs

'''
Set global variables and important lists of stuff like links to unshorten, or
those to keep query terms on, etc.
'''

MAX_THREADS = 20

'''
Define module-level functions and classes
'''

class get_rumble_data_from_urls:
    
    def __init__(self, path_to_chromedriver, save_full_video=False, video_save_dir="rumble_videos", 
                 save_img_thumbnail=False, img_save_dir="rumble_thumbnails",
                 num_tries=3):
        
        self.video_save_dir = video_save_dir
        self.img_save_dir = img_save_dir
        self.save_full_video = save_full_video
        self.save_img_thumbnail = save_img_thumbnail
        self.num_tries = num_tries
        self.path_to_chromedriver = path_to_chromedriver
        
        #create video directory, if needed
        if save_full_video:
            if video_save_dir == "rumble_videos":
                if video_save_dir not in os.listdir():
                    try:
                        os.mkdir(video_save_dir)
                    except OSError:
                        pass
                
        #create image directory, if needed
        if save_img_thumbnail:
            if img_save_dir == "rumble_thumbnails":
                if img_save_dir not in os.listdir():
                    try:
                        os.mkdir(img_save_dir)
                    except OSError:
                        pass
                
        
    def retrieve_from_url(self, url):
        
        if self.save_full_video: 
            videos_already_downloaded = [i[:-4] for  i in os.listdir(self.video_save_dir)]
        else:
            videos_already_downloaded = []
        
        datum={
            "rumble_url": url,
            "video_id": "None",
            "video_url": "None",
            'author': "None",
            'publish_date': "None",
            "views": "None",
            "rumbles_count": "None",
            'description': "None",
            "money_earned": "None"
            }
        
        if '/embed/' in url:
            try:
                html = requests.get(url, headers={"User-Agent": "Requests"}).content
                soup = BeautifulSoup(html, 'html.parser')
                url = soup.find("link", {"rel":"canonical"}).get("href")
            except:
                logging.error(url+" : unable to extract base url for webpage")
        for i in range(self.num_tries):
            try:
                options = Options()
                options.add_argument('--headless')
                try:
                    with webdriver.Chrome(self.path_to_chromedriver, options=options) as driver:
                        driver.set_page_load_timeout(30)
                        driver.get(url)
                        html = driver.page_source
                except:
                    logging.exception("problem with webdriver: ")
                   
                try:
                    soup = BeautifulSoup(html, 'html.parser')
                    video_url = soup.find("div", {"class":"videoPlayer-Rumble-cls"}).find("video").attrs["src"]
                    video_id = urlparse(video_url).path.split("/")[-1]
                except:
                    logging.exception("problem with soup: ")
                datum["video_id"] = video_id
                datum["video_url"] = video_url
                
            except:
                logging.error("unable to get video webpage: "+url)
                logging.exception("Exception: ")
                time.sleep(random.uniform(30,120))
                
            else:
                try:
                    datum['author'] = soup.find("span", {"class":"media-heading-name"}).text.strip()
                except:
                    pass
                try:
                    datum['publish_date'] = soup.find("span", {"class":"media-heading-info media-heading-published"}).text.strip()[11:]
                except:
                    pass
                try:
                    datum['views'] = soup.find_all("span", {"class":"media-heading-info"})[-1].text[:-6]
                except:
                    pass
                try:
                    datum["money_earned"] = soup.find("span", {"class":"media-earnings"}).text.strip()
                except:
                    pass
                try:
                    datum["rumbles_count"] = soup.find("span", {"class":"rumbles-count"}).text.strip()
                except:
                    pass
                try:
                    datum['description'] = soup.find("p", {"class":"media-description"}).text.strip()
                except:
                    pass
                
                if self.save_img_thumbnail:
                    try:
                        thumb_url = soup.find("div", {"class":"videoPlayer-Rumble-cls"}).find("video").attrs["poster"]
                        with open(os.path.join(self.img_save_dir, video_id+thumb_url[-4:]), 'wb') as f:
                            response = requests.get(thumb_url)
                            f.write(response.content)
                    except:
                        logging.error("Unable to access thumbnail image: "+thumb_url)
                
                
                
                # Save out the full video, if desired
                if self.save_full_video:
                    if video_id not in videos_already_downloaded:
                        try:
                            video = requests.get(video_url, stream=True)
                            with open(os.path.join("rumble_videos",video_id), 'wb') as fd:
                                for chunk in video.iter_content(chunk_size=1024):
                                    fd.write(chunk)
                        except:
                            logging.error(url+" : unable to download video content")
                
        return datum
        
        
    def retreive_from_urls(self, list_of_urls):
        threads = min(MAX_THREADS, len(list_of_urls))
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            processed_urls =executor.map(self.retrieve_from_url, list_of_urls)
            
        return pd.DataFrame(processed_urls)