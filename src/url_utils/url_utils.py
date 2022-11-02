# -*- coding: utf-8 -*-
"""
Created on Tue Oct 26 15:45:18 2021

@author: icruicks
"""
from abc import ABC, abstractmethod
import requests, urllib3, re, os, logging, time, random, string
import concurrent.futures
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from bs4 import BeautifulSoup 
from newsplease import NewsPlease
try:
    from unshortenit import UnshortenIt
    unshortener_available = True
except:
    unshortener_available = False
from urllib.parse import urljoin, urlunparse, urlparse, parse_qs

'''
Set global variables and important lists of stuff like links to unshorten, or
those to keep query terms on, etc.
'''

MAX_THREADS = 20

LINK_SHORTENERS =set(['trib.al', 'bit.ly','www.bit.ly','tinyurl','ow.ly','buff.ly',
                           'rebrand.ly', 'dlvr.it','sco.lt', 'shar.es', 'spr.ly',
                           'zpr.io', 'zurl.co', 'tinyurl.com', 'ht.ly', 'youtu.be',
                           't.ly', 'snip.ly', 'qoo.ly', 'loom.ly', 'invst.ly',
                           'hubs.ly', 'gates.ly', 'frost.ly', 'fcld.ly', 'cutt.ly',
                           'all.be', 'amzn.to', 'goo.gl', 'is.gd', 'bit.do', 'mcaf.ee',
                           'shorte.st', 'bc.vc', 'lnkd.in', 't.co', 'ift.tt', 'flip.it',
                           'reut.rs', 'nyti.ms', 'chng.it', 'cnn.it', 'cnb.cx', 'mol.im',
                           'paper.li', 'toi.in', 'flip.it', 'hill.cm', 'bbc.in',
                           'ti.me', 'politi.co', 'aje.io', 'gizmo.do', 'tiny.iavian.net',
                           'w.wiki', 'w-j.com', 'wp.lnjmp.com', 'hann.it', 'feedproxy.google.com',
                           'tiny.iavian.com'])

FALSE_LINK_SHORTENERS = set( ["t.me"
                              
                              
                              ])

KEEP_QUERY_TERMS = ['www.youtube.com', 'youtube.com', 'm.youtube.com', 'youtu.be', 
                    'None','m.facebook.com', 'facebook.com','theresistance.video']


'''
Define the top-level abstract class for the various URL data utilities. The general
format consist in setting the parameters of the collection, the actual code for
the collection itself, and multi-threading the actual collection piece.
'''

class internet_data_collection(ABC):
 
    @abstractmethod
    def retrieve_from_url(self, url):
        pass
    
    def retreive_from_urls(self, list_of_urls):
        threads = MAX_THREADS
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            collection_results = executor.map(self.retrieve_from_url, list_of_urls)
                
        return pd.DataFrame(collection_results)
    
'''
Define module-level functions
'''

def get_domain(url):
    '''
    Gets the high-level domain of a URL

    Parameters
    ----------
    url : string
        URL of a website

    Returns
    -------
    domain : string
        High-level domain of that website
    '''
    
    if url !='None':
        domain = urlparse(url).netloc.lower()
        if domain[0:4] =='www.':
            domain = domain[4:]
    else:
        domain = 'None'
    return domain


def map_mobile_to_original(url):
    """
    check for whether a url is a mobile url, and then map it back to
    the orginal website.
  
    Parameters
    ----------
    url : string 
        url of website

    Returns
    -------
    new_url : str
        url that is not a mobile type
    mobile : bool 
        wether the url was a mobile type
  
    """
    u = urlparse(url)
    if u.path.lower()[0:2] == 'm.':
        new_url = urlunparse( u._replace(path = u.path.lower()[2:]))
        mobile= True
    elif u.netloc.lower()[0:2] == 'm.':
        new_url = urlunparse(u._replace(netloc = u.netloc.lower()[2:]))
        mobile = True
    else:
        new_url = url
        mobile = False
    return new_url, mobile


def remove_query_terms(url, keep_query_terms=KEEP_QUERY_TERMS):
    """
    check for whether a url has query terms ('?'), and if not a website
    that uniuely identifies content by query terms, remove them
  
    Parameters
    ----------
    url : string
        URL of a website
    keep_query_terms : set of str 
        set or other iterable of domains that
        need to keep query terms NOTE: uses globally variable as default.
  
    Returns
    -------
    new_url : str
        url without query terms
  
    """
    query = urlparse(url)
    if (query.path.lower() not in keep_query_terms) & (query.netloc.lower() not in keep_query_terms):
        new_url = urljoin(url, query.path)
    elif query.hostname in ('www.youtube.com', 'youtube.com', 'm.youtube.com'):
        p = parse_qs(query.query)
        video_id = p.get('v',['None'])[0]
        new_url = query.scheme+"://"+query.netloc+query.path+"?v="+video_id
    else:
        new_url = url
    return new_url
    
    
def get_base_url(url, link_shorteners=LINK_SHORTENERS, false_link_shorteners = FALSE_LINK_SHORTENERS, num_tries=3):
    """
    check for whether a url is shortened, and then unshorten
    it if it is.
  
    Parameters
    ----------
    url : string
        URL of a website
    link_shorteners : set of str 
        set or other iterable of link shortener domains 
        to check for. NOTE: uses globally variable as default.
  
    Returns
    -------
    new_url : str
        url without query terms
    shortened : bool
        wether the url was a shortened url
  
    """
    
    url_netloc = urlparse(url).netloc.lower()
    if ((url_netloc in link_shorteners) | (bool(re.match(r"\.\w\w", url_netloc[-3:])))) & (url_netloc not in false_link_shorteners):
        shortened = True
        if unshortener_available:
            for i in range(num_tries):
                try:
                    unshortener = UnshortenIt(default_timeout=20)
                    base_url = unshortener.unshorten(url, unshorten_nested=True,
                                                     force=True)
                except:
                    logging.error("unable to unshorten: "+url)
                    base_url = url
                    time.sleep(random.uniform(30,120))
                else:
                    return base_url, shortened
        else:
            urllib3.disable_warnings()
            session = requests.Session()
            for i in range(num_tries):
                try:
                    resp = session.head(url, allow_redirects=True, timeout=20.0, verify=False)
                    base_url = resp.url
                except:
                    logging.error("unable to unshorten: "+url)
                    base_url = url
                    time.sleep(random.uniform(30,120))
                else:
                    return base_url, shortened
        return base_url, shortened
    else:
        shortened = False
        return url, shortened
    
    
'''
Define module-level classes that inheret from internet_data_collection
abstract class
'''
    
class process_urls(internet_data_collection):
    
    def __init__(self, unshorten=True, map_mobile=True):
        self.unshorten = unshorten
        self.map_mobile = map_mobile
        
    def retrieve_from_url(self, url):
        datum ={}
        datum['original_url'] = url
        if self.unshorten:
            url, datum['shortened'] = get_base_url(url)
            
        url = remove_query_terms(url)
        
        if self.map_mobile:
            url, datum['mobile'] = map_mobile_to_original(url)
            
        datum['domain'] = get_domain(url)
        datum['base_url'] = url
        return datum
    
    def retreive_from_urls(self, list_of_urls):
        threads = min(len(list_of_urls), MAX_THREADS)
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            collection_results = executor.map(self.retrieve_from_url, list_of_urls)
                
        return pd.DataFrame(collection_results)
    
    
class get_text_from_urls(internet_data_collection):
    
    def __init__(self, num_retries=3, path_to_chromedriver=None):
        self.num_retries = num_retries
        self.path_to_chromedriver = path_to_chromedriver
        if path_to_chromedriver is not None:
            self.use_selenium= True
        else:
            self.use_selenium = False
    '''        
    def retrieve_from_url(self, url):
        datum= {'url':url}
        html = None
        logging.info("Getting text from: "+url)
        for attempt in range(self.num_retries):
            try:
                html = requests.get(url, headers={"User-Agent": "Requests"},
                                    allow_redirects=True, timeout=10).content
            except requests.exceptions.SSLError:
                try:
                    html = requests.get(url, headers={"User-Agent": "Requests"},
                                        allow_redirects=True, timeout=10, verfiy=False).content
                    break
                except:
                    break
            except requests.exceptions.RequestException:
                time.sleep(random.uniform(30,120))
            else:
                break
            
        if html == None:
            logging.error("Unable to get text by requests for: "+url)
            if self.use_selenium:
                try:
                    options = Options()
                    options.add_argument('--headless')
                    with webdriver.Chrome(self.path_to_chromedriver, options=options) as driver:
                        driver.set_page_load_timeout(30)
                        driver.get(url)
                        raise WebDriverException
                    html = driver.page_source
                except:
                    logging.error("Unable to get html from website: "+url)
                    datum['text']= "exception occurred with accessing url"
                    return datum
            else:
                datum['text']= "exception occurred with accessing url"
                return datum
    
        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = ''
            for tag in soup.find_all(['p', 'text']):
                text = text + ' ' + re.sub(r'\n\s*\n', r' ', tag.get_text().strip(), flags=re.M)
                if len(text) > 3000000:
                    break
                
        except:
            logging.error("Unable to parse result: "+url)
            datum['text']= "exception occurred with html parsing"
        else:
            if len(text) <=500:
                datum['text'] = "unable to extract meaningful text"
            else:
                datum['text']= text
    
        return datum
    '''
    def retrieve_from_url(self, url):
        datum= {'url':url}
        article = None
        logging.info("Getting text from: "+url)
        for attempt in range(self.num_tries):
            try:
                article = NewsPlease.from_url(url, timeout=10).get_dict()
            except:
                logging.error("Unable to scrape: "+url)

            if article == None:
                datum['text']= "error in scraping"
                datum['date_publish'] = None
                datum['image_url'] = None
                datum['language'] = None
                datum['text'] = None
                datum['title'] = None
                break
            else:
                datum['text']= "error in scraping"
                datum['date_publish'] = article['date_publish']
                datum['image_url'] = article['image_url']
                datum['language'] = article['language'] 
                datum['text'] = article['maintext']
                datum['title'] = article["title"]
                break


        return datum
    
            
    def retreive_from_urls(self, list_of_urls):
        threads = min(MAX_THREADS, len(list_of_urls))
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            processed_urls =executor.map(self.retrieve_from_url, list_of_urls)
            
        return_df = pd.DataFrame(processed_urls)
        
        logging.info("succesfully created df: {} and {}".format(list_of_urls[0], list_of_urls[-1]))
       
        return return_df
            
            
class get_images_from_urls(internet_data_collection):
            
    def __init__(self, num_retries=3, path_to_chromedriver=None, 
                 image_save_directory="images"):
        self.num_retries = num_retries
        self.path_to_chromedriver = path_to_chromedriver
        if path_to_chromedriver is not None:
            self.use_selenium= True
        else:
            self.use_selenium = False

        self.image_save_directory = image_save_directory
        if image_save_directory == "images":        
            if image_save_directory not in os.listdir():
                try:
                    os.mkdir(image_save_directory)
                except OSError:
                    pass
            
    def retrieve_from_url(self, url):
        datum= {'url':url,
                'image_urls':[],
                'image_names':[]}
        html = None
        logging.info("Getting image from: "+url)
        for attempt in range(self.num_retries):
            try:
                html = requests.get(url, headers={"User-Agent": "Requests"},
                                    allow_redirects=True, timeout=10).content
            except requests.exceptions.SSLError:
                try:
                    html = requests.get(url, headers={"User-Agent": "Requests"},
                                        allow_redirects=True, timeout=10, verfiy=False).content
                    break
                except:
                    break
            except requests.exceptions.RequestException:
                time.sleep(random.uniform(30,120))
            else:
                break
            
        if html == None:
            logging.error("Unable to get images by requests for: "+url)
            if self.use_selenium:
                try:
                    options = Options()
                    options.add_argument('--headless')
                    with webdriver.Chrome(self.path_to_chromedriver, options=options) as driver:
                        driver.get(url)
                        driver.set_page_load_timeout(30)
                        raise WebDriverException
                    html = driver.page_source
                except:
                    logging.error("Unable to get html from website: "+url)
                    datum['image_name']= "None"
                    return datum
            else:
                datum['image_name']= "None"
                return datum
    
        try:
            soup = BeautifulSoup(html, 'html.parser')
            img_tags = soup.find_all('img')
            urls = [img['src'] for img in img_tags]
            datum['image_urls'] = urls
            image_filenames = [''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(6))+i[-4:] for i in urls]
            datum['image_names'] = image_filenames
            for i in range(len(urls)):
                image_url = urls[i]
                image_filename = image_filenames [i]
                try:
                    with open(os.path.join(self.image_save_directory, image_filename), 'wb') as f:
                        if 'http' not in url:
                            # sometimes an image source can be relative 
                            # if it is provide the base url which also happens 
                            # to be the site variable atm. 
                            image_url = '{}/{}'.format(url, image_url)
                        response = requests.get(image_url)
                        f.write(response.content)
                except:
                    logging.error("Unable to access image: "+image_url)
                
        except:
            logging.error("Unable parse images from html: "+url)
    
        return datum
            
    
    def retreive_from_urls(self, list_of_urls):
        threads = min(MAX_THREADS, len(list_of_urls))
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            processed_urls =executor.map(self.retrieve_from_url, list_of_urls)
            
        return pd.DataFrame(processed_urls)
            