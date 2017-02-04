import multiprocessing
import time
import threading
import urllib

from mongo_cache import MongoCache
from mongo_queue import MongoQueue
from downloader import Downloader


DEFAULT_DELAY = 5
DEFAULT_AGENT = 'wswp'
DEFAULT_RETRY = 1
DEFAULT_THREAD = 10
DEFAULT_TIMEOUT = 60
SEEP_TIME = 1


def threaded_crawler(seed_url, delay=DEFAULT_DELAY, cache=None,
  scrape_callback=None, user_agent=DEFAULT_AGENT, proxies=None,
    num_retries=DEFAULT_RETRY, max_threads=DEFAULT_THREAD,
      timeout=DEFAULT_TIMEOUT):
  '''Crawl using multiple threads
  '''
  # the queue of URL's that still need to be crawled
  crawl_queue = MongoQueue()
  crawl_queue.clear()
  crawl_queue.push(seed_url)
  D = Downloader(cache=cache, delay=delay, user_agent=user_agent,
    proxies=proxies, num_retries=num_retries, timeout=timeout)
    
  def process_queue():
    while True:
      # keep track that are processing url
      try:
        url = crawl_queue.pop()
      except KeyError:
        # currently no urls to process
        break
      else:
        html = D(url)
        if scrape_callback:
          try:
            links = scrape_callback(url, html) or []
          except Exception as e:
            print('Error in callback for: {}: {}'.format(url, e))
          else:
            for link in links:
              # add this new link to queue
              crawl_queue.push(norialize(seed_url, link))
        crawl_queue.complete(url)
        
  # wait for all download threads to finish
  threads = []
  while threads or crawl_queue:
    for thread in threads:
      if not thread.is_alive():
        threads.remove(thread)
    while len(threads) < max_threads and crawl_queue.peek():
      # can start some more threads
      thread = threading.Thread(target=process_queue)
      thread.setDaemon(True) # set daemon so main thread can exit when receives ctrl-c
      thread.start()
      threads.append(thread)
    time.sleep(SLEEP_TIME)
    
def process_crawler(args, **kwargs):
  num_cpus = multiprocessing.cpu_count()
  # pool = multiprocessing.Pool(processes=num_cpus)
  print('Starting {} processes'.format(num_cpus))
  processes = []
  for i in range(num_cpus):
    p = multiprocessing.Process(target=threaded_crawler, args=[args],
      kwargs=kwargs)
    # parsed = pool.apply_async(threaded_link_crawler, args, kwargs)
    p.start()
    processes.append(p)
  # wait for processes to complete
  for p in processes:
    p.join()
    
def normalize(seed_url, link):
  '''Normalize this URL by removing hash and adding domain
  '''
  link, _ = urllib.parse.urldefrag(link) # remove hash to avoid duplicates
  return urlparse.urljoin(seed_url, link)
