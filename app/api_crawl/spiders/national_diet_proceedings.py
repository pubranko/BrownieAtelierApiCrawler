import scrapy
from datetime import datetime, date
from datetime import datetime, timedelta
from typing import Union, Any
from scrapy.http.request.form import FormRequest
from scrapy.http.response.json import JsonResponse
from scrapy.http.response import Response
from BrownieAtelierMongo.collection_models.api_crawler_response_model import ApiCrawlerResponseModel
from api_crawl.items import ApiCrawlItem
from api_crawl.api_crawl_input import ApiCrawlInput
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel

class NationalDietProceedingsSpider(scrapy.Spider):
    name = "national_diet_proceedings"
    allowed_domains = ["kokkai.ndl.go.jp"]
    start_urls = []

    api_crawl_input: ApiCrawlInput

    start_date: date
    end_date: date

    # MongoDB関連
    mongo: MongoModel  # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 文字列をdate型に変換
        
        self.start_date = date.fromisoformat(kwargs["start_date"])
        self.end_date = date.fromisoformat(kwargs["end_date"])
        
        self.api_crawl_input = ApiCrawlInput(
            start_date=self.start_date,
            end_date=self.end_date,
        )
        self.page = 0
        # 
        current_date:date = self.start_date
        while current_date <= self.end_date:
            self.start_urls.append(self.build_url(current_date))

            current_date += timedelta(days=1)

        self.logger.info(f"start_urls生成: {self.start_urls}")

        # MongoDBオープン
        self.mongo = MongoModel(
            self.logger.logger
        )  # MongoModelではLoggerAdapterではなくLoggerで定義している。そのためとりあえずLoggerを渡すよう対応中


    def start_requests(self):
        self.logger.info(f"start_requests起動")

        if not self.start_urls:
            self.logger.warning("self.start_urlsが空です。")
            return
        
        for url in self.start_urls:
            any: Any = self.parse   # コードチェックでワーニングが出ないようにAnyとしている。
            yield scrapy.Request(url, callback=any)

    def build_url(self, target_date:date):
        base_url = "https://kokkai.ndl.go.jp/api/meeting"
        params = {
            "from": target_date,
            "until": target_date,
            "recordPacking": "json",
        }
        query_string = "&".join(f"{key}={value}" for key, value in params.items() if value)
        return f"{base_url}?{query_string}"


    def parse(self, response: JsonResponse):

        number_of_records = response.json()["numberOfRecords"]   # 会議体の件数。会議がない日はゼロとなる。
        if not number_of_records:
            self.logger.info(f"取得した会議録はありませんでした: {response.url} ")
        else:
            for meeting_record in response.json()["meetingRecord"]:

                yield ApiCrawlItem(
                    domain = self.allowed_domains[0],
                    url = response.url,
                    crawling_start_time = self.api_crawl_input.crawling_start_time,
                    response_time = datetime.now().astimezone(self.settings["TIMEZONE"]),
                    response = meeting_record,
                )
