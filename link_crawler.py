import datetime
import re
import time
import urllib
from urllib import robotparser
from urllib.request import urlparse

from downloader import Downloader


DEFAULT_DELAY = 5
DEFAULT_DEPTH = -1
DEFAULT_URL = -1
DEFAULT_AGENT = 'wswp'
DEFAULT_PROXY = -1
DEFAULT_RETRY = 1


def link_crawler(seed_url, link_regex=None, delay=DEFAULT_DELAY, max_depth=DEFAULT_DEPTH,
  max_urls=DEFAULT_URL, user_agent=DEFAULT_AGENT, proxies=DEFAULT_PROXY, num_retries=DEFAULT_RETRY,
    scrape_callback=None, cache=None):
  '''
    Crawl from the given seed URL following links matched by link_regex
  '''
  # the queue of URL's that still need to be crawled
  crawl_queue = [seed_url]
  # the URL's that have been seen and at what depth
  seen = {seed_url: 0}
  # track how many URL's have been downloaded
  num_urls = 0
  rp = get_robots(seed_url)
  D = Downloader(delay=delay, user_agent=user_agent, proxies=proxies,
    num_retries=num_retries, cache=cache)
    
  while crawl_queue:
    url = crawl_queue.pop()
    depth = seen[url]
    # check url passes robots.txt restrictions
    if rp.can_fetch(user_agent, url):
      html = D.download(url)['html'].decode()
      links = []
      if scrape_callback:
        links.extend(scrape_callback(url, html) or [])
        
      if depth != max_depth:
        # can still crawl further
        if link_regex:
          # filter for links matching our regular expression
          links.extend(link for link in get_links(html) if \
            re.match(link_regex, link))
           
        for link in links:
          link = normalize(seed_url, link)
          # check whether already crawled this link
          if link not in seen:
            seen[link] = depth + 1
            # check link is within same domain
            if same_domain(seed_url, link):
              # success add this new link to queue
              crawl_queue.qppend(link)
              
      # check whether have reached downloaded maximum
      num_urls += 1
      if num_urls == max_urls:
        break
        
    else:
      print('Blocked by robots.txt', url)
      
      
def normalize(seed_url, link):
  '''
    Normalize this URL by removing hash and adding domain
  '''
  link, _ = urllib.parse.urldefrag(link) # remove hash to avoid duplicates
  return urllib.parse.urljoin(seed_url, link)
  
  
def same_domain(url1, url2):
  '''
    Return True if both URL's belong to same domain
  '''
  return urllib.parse.urlparse(url1).netloc == urllib.parse.urlparse(url2).netloc
  

def get_robots(url):
  '''
    Initialize robots parser for this domain
  '''
  rp = robotparser.RobotFileParser()
  rp.set_url(urllib.parse.urljoin(url, '/robots.txt'))
  rp.read()
  return rp
  
  
def get_links(html):
  '''
    Return a list of links from html
  '''
  # a regular expression to extract all links from the webpage
  webpage_regex = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
  # list of all links from the webpage
  return webpage_regex.findall(html)
  

if __name__ == '__main__':
  # execute only if run as a script
  pass