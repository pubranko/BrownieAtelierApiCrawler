# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from BrownieAtelierMongo.collection_models.api_crawler_response_model import \
    ApiCrawlerResponseModel


class ApiCrawlItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    _id = scrapy.Field()
    domain = scrapy.Field()
    url = scrapy.Field()
    crawling_start_time = scrapy.Field()
    response_time = scrapy.Field()
    response = scrapy.Field()

    def __repr__(self):
        # 当クラスのurl,title,contentを引数に、当クラスのインスタンス化をしているようだ。
        # ログからresponse_headers,response_bodyを削除するために細工
        p: ApiCrawlItem = ApiCrawlItem(self)
        del p[ApiCrawlerResponseModel.DOMAIN]
        del p[ApiCrawlerResponseModel.URL]
        del p[ApiCrawlerResponseModel.CRAWLING_START_TIME]
        del p[ApiCrawlerResponseModel.RESPONSE_TIME]
        del p[ApiCrawlerResponseModel.RESPONSE]
        return super(ApiCrawlItem, p).__repr__()
