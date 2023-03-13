from scrapy.cmdline import execute
execute('scrapy crawl ncbi_crawler'.split())

# from scrapy.crawler import CrawlerProcess
# from scrapy.utils.project import get_project_settings
#
# settings = get_project_settings()
#
# crawler = CrawlerProcess(settings)
#
# crawler.crawl('ncbi_crawler')
# crawler.crawl('ncbi_bioSample_cralwer')
#
# crawler.start()
# crawler.start()