import time
import threading
import urllib

from downloader import Downloader

DEFAULT_DELAY=5
DEFAULT_AGENT='wswp'
DEFAULT_RETRY=1
DEFAULT_THREADS=10
DEFAULT_TIMEOUT=60
SLEEP_TIME=1


def threaded_crawler(seed_url, delay=DEFAULT_DELAY, cache=None, 
  scrape_callback=None, user_agent='wswp', proxies=None,
    num_retries=DEFAULT_RETRY, max_threads=DEFAULT_THREADS, 
      timeout=DEFAULT_TIMEOUT):
  '''Crawl this website in multiple threads
  '''
  # the queue of URL's that still need to be crawled
  # crawl_queue = Queue.deque([seed_url])
  crawl_queue = [seed_url]
  # the URL's that have been seen
  seen = set([seed_url])
  D = Downloader(cache=cache, delay=delay, user_agent=user_agent,
    proxies=proxies, num_retries=num_retries, timeout=timeout)
    
  def process_queue():
    while True:
      try:
        url = crawl_queue.pop()
      except IndexError:
        # crawl queue is empty
        break
      else:
        html = D(url)
        if scrape_callback:
          try:
            links = scrape_callback(url, html) or []
          except Exception as e:
            print('Error in callback for : {}: {}'.format(url, e))
          else:
            for link in links:
              link = normalize(seed_url, link)
              # check whether already crawled this link
              if link not in seen:
                seen.add(link)
                # add this new link to queue
                crawl_queue.append(link)
                
  # wait for all download threads to finish
  threads = []
  while threads or crawl_queue:
    # the crawl is still active
    for thread in threads:
      if not thread.is_alive():
        # remove the stopped threads
        threads.remove(thread)
    while len(threads) < max_threads and crawl_queue:
      # can start some more threads
      thread = threading.Thread(target=process_queue)
      thread.setDaemon(True) # set daemon so main thread can exit when receives ctrl-c
      thread.start()
      threads.append(thread)
    # all threads have been processed
    # sleep temporarily so CPU focus execution on other threads
    time.sleep(SLEEP_TIME)
    
    
def normalize(seed_url, link):
  '''Normalize this URL by removing hash and adding domain
  '''
  link, _ = urllib.parse.urldefrag(link) # remove hash to avoid duplicates
  return urllib.parse.urljoin(seed_url, link)
