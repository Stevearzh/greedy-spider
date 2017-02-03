import csv
from io import StringIO
from zipfile import ZipFile

from mongo_cache import MongoCache


DEFAULT_MAX_URLS=1000


class AlexaCallback:
  def __init__(self, max_urls=DEFAULT_MAX_URLS):
    self.max_urls = max_urls
    self.seed_url = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
    
  def __call__(self, url, html):
    if url == self.seed_url:
      urls = []
      cache = MongoCache()
      with ZipFile(StringIO(html)) as zf:
        csv_filename = zf.namelist()[0]
        for _, website in csv.reader(zp.open(csv_filename)):
          if 'http://' + website not in cache:
            urls.append('http://' + website)
            if len(urls) == self.max_urls:
              break
      return urls
