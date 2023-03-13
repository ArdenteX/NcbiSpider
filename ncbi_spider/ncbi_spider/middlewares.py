# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
import requests
from scrapy.http import HtmlResponse
from settings import HEADER
from RedisAPI import RedisUrl
from RedisAPI import compose_url
from lxml import html

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class NcbiSpiderSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class NcbiSpiderDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.
    def __init__(self):
        self.redisOperator = RedisUrl(redis_name='ncbi:error_urls')

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        spider.logger.info("中间件捕获到url ---------> %s" % request.url)
        if spider.name == 'ncbi_bioSample_cralwer':
            n_u = request.url.split('%3E')
            url = n_u[1]

        else:
            url = request.url

        error_times = 0
        while True:
            try:
                res = requests.get(url, headers=HEADER, timeout=8)

                # Test
                t_etree = html.etree.HTML(res.content)
                try:
                    str(t_etree.xpath("//div[@id='summary']/h1/text()")[0])

                except Exception as e:
                    spider.logger.info("Empty Item, Retrying.....")
                    continue

                error_times = 0
                spider.logger.info("Getting Finished")
                break

            except requests.exceptions.RequestException:
                error_times += 1
                if error_times >= 3:
                    if spider.name == 'ncbi_bioSample_cralwer':
                        compose_url(url, 'ncbi:error_url_bio')
                    self.redisOperator.add(url)
                    spider.logger.info("Put this url into redis: %s" % url)

                else:
                    spider.logger.info("Retrying Connect...")
                    continue

        res.encoding = 'utf-8'
        response = HtmlResponse(url=request.url)
        if spider.name == 'ncbi_bioSample_cralwer':
            response.metas = {'text': res.content, 'name': n_u[0]}

        else:
            response.metas = {'text': res.content}
        return response

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
