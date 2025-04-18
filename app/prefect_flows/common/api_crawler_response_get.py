from typing import Generator
from datetime import datetime
from prefect import get_run_logger
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.api_crawler_response_model import ApiCrawlerResponseModel

def api_crawler_response_get(
    mongo: MongoModel,
    target_domain: str,
    target_start_time_from: datetime,
    target_start_time_to: datetime,
) -> Generator:
    """
    Apiクローラーレスポンスより国会会議録を取得するgeneratorを返す。
    """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(f"api_crawler_response_get 開始 ({target_domain} - {target_start_time_from} - {target_start_time_to})")

    api_crawler_response = ApiCrawlerResponseModel(mongo)

    conditions: list = []
    conditions.append({ApiCrawlerResponseModel.DOMAIN: target_domain})
    conditions.append(
        {ApiCrawlerResponseModel.CRAWLING_START_TIME: {"$gte": target_start_time_from}}
    )
    conditions.append(
        {ApiCrawlerResponseModel.CRAWLING_START_TIME: {"$lte": target_start_time_to}}
    )
    filter = {"$and": conditions}

    documents_generator: Generator = api_crawler_response.limited_find(
        # projection = None,
        filter = filter,
        # sort = None
    )

    logger.info(f"=== 取得件数: {api_crawler_response.count_documents(filter = filter)}")

    return documents_generator  
