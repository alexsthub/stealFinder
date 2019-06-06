import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib
import asyncio
import aiohttp

import json


# handle multiple pages.

class craigslistScraper(object):
    def __init__(self, baseUrl, endingUrl):
        self.baseUrl = baseUrl
        self.endingUrl = endingUrl
        self.pageNum = 0
        self.newUrl = None

    def paginate(self, baseUrl, pageNum):
        self.newUrl = self.baseUrl.replace('<PageNum>', str(pageNum))
        return self.newUrl

    def scrape(self, query):
        # TODO: Create base df
        ret = pd.DataFrame(columns = ['id', 'title', 'price', 'descr', 'link', 'images'])
        while True:
            urls = self.getPageItems(query)
            if urls is None:
                break
            df = asyncio.run(self.crawlProcess(urls))
            ret = ret.append(df)
            self.pageNum += len(df)
        return ret
        
    def buildQuery(self, query):
        self.newUrl = self.paginate(self.baseUrl, self.pageNum)
        query = query.replace(' ', '+')
        return self.newUrl + query + self.endingUrl

    def getPageItems(self, query):
        urls = []
        urlString = self.buildQuery(query)
        print(urlString)
        response = requests.get(urlString)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find_all('li', class_='result-row')
        if len(table) == 0:
            return None
        for row in table:
            urls.append(row.find('a')['href'])
        return urls

    async def crawlProcess(self, urls):
        tasks = []
        connector = aiohttp.TCPConnector(limit = 50, ssl = False)
        timeout = aiohttp.ClientTimeout(total = 10000000) # arbitrary large number
        print('There are %i urls to process' % len(urls))
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            for url in urls:
                tasks.append(self.extractMetadata(session, url))
            ret = await asyncio.gather(*tasks, return_exceptions = True)
        await session.close()
        print('There are %i responses' % len(ret))
        df = pd.DataFrame(ret, columns = ['id', 'title', 'price', 'descr', 'link', 'images'])
        return df

    async def getTime(self, soup):
        rawTime = soup.find('time')['datetime']
        rawTime = rawTime[:rawTime.rfind(':')]
        datetime_object = datetime.strptime(rawTime, '%Y-%m-%dT%H:%M')
        return datetime_object

    async def extractMetadata(self, session, url):
        ret = {}
        async with session.get(url, allow_redirects = False) as response:
            soup = BeautifulSoup(await response.text(), 'html.parser')
            titleContainer = soup.find('span', class_='postingtitletext')
            title = titleContainer.find('span', id ='titletextonly').text
            if titleContainer.find('span', class_ = 'price'):
                price = float(titleContainer.find('span', class_ = 'price').text.replace('$',''))
            else:
                price = 0.0
            
            time = await self.getTime(soup)
            
            postID = url[url.rfind('/') + 1:-5]
            
            # TODO: find a better way to strip out \n in description
            # How to do it efficiently? Strings are immutable
            descr = soup.find('section', id='postingbody').text.replace('\n\nQR Code Link to This Post\n\n\n', '').strip().replace('\n','')
        
            # How to get image? Send headless request?
            # Just grab img urls for now
            images = []
            
            ret['title'] = title
            ret['id'] = postID
            ret['price'] = price
            ret['time'] = time
            ret['descr'] = descr
            ret['link'] = url
            ret['images'] = ','.join(images)
            
            return ret

def getConfig(retailer, crawlType, isRetailer = True):
    # TODO : Error handling
    with open('config.json') as f:
        obj = json.load(f)
    obj = obj['retailers'][retailer][crawlType]
    config = {}
    for key in obj.keys():
        config[key] = obj[key]
    return config

if __name__ == "__main__":
    config = getConfig('craigslist', 'fpr')
    scraper = craigslistScraper(config['baseUrl'], config['endUrl'])
    query = 'ac unit'
    df = scraper.scrape(query)

    df.to_csv('test.csv', index = False)
    print('All Done!')