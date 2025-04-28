from typing import Generator
from datetime import datetime
from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE
from BrownieAtelierMongo.common.api_crawler_response_get import api_crawler_response_get
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.api_crawler_response_model import ApiCrawlerResponseModel as ApiResponse
from BrownieAtelierMongo.collection_models.national_diet_proceedings_master_model import NationalDietProceedingsMasterModel as Master
# from pymongo.cursor import Cursor

@task(cache_policy=NO_CACHE)    # cache_policy=NO_CACHE タスクのキャッシュ機能を無効化。内部でシリアライズできないkey(mongo)があるとエラーとなるため。
def national_diet_proceedings_master_save_task(mongo: MongoModel, target_start_time_from: datetime, target_start_time_to: datetime,):
    """
    Apiクローラーレスポンスより国会会議録を取得し、国会会議録マスターへ保存する。
    ただし同一データの場合は保存をスキップする。
    """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(f"national_diet_proceedings_master_save_task 開始 ({target_start_time_from} - {target_start_time_to})")

    api_crawler_response_get_generator: Generator = api_crawler_response_get(
        mongo, Master.DOMAIN_VALUE, target_start_time_from, target_start_time_to)

    skipped_count: int = 0
    saved_count: int = 0
    for api_crawler_document in api_crawler_response_get_generator:

        # api経由で取得したレスポンスデータを取得
        response = api_crawler_document[ApiResponse.RESPONSE]

        conditions: list = []
        # apiレスポンス内の会議日、院名、会議名、号数が一致するものを取得
        conditions.append({Master.DATE: response[Master.DATE]})
        conditions.append({Master.NAME_OF_HOUSE: response[Master.NAME_OF_HOUSE]})
        conditions.append({Master.NAME_OF_MEETING: response[Master.NAME_OF_MEETING]})
        conditions.append({Master.ISSUE: response[Master.ISSUE]})
        filter = {"$and": conditions}

        master_model = Master(mongo)

        master_records =  master_model.find(
            # projection = None, 
            filter = filter, 
            # sort = None
        )
    
        skip_flag = False
        for master_record in master_records:
            # スピーチ全量を丸ごと比較
            if (master_record[Master.SPEECH_RECORD] == response[Master.SPEECH_RECORD]):
                skip_flag = True
                break

        if skip_flag:
            skipped_count += 1
            continue

        # 保存用のデータを作成
        save_data = {}
        
        # 基本情報をコピー
        save_data[Master.CRAWLING_START_TIME] = api_crawler_document[ApiResponse.CRAWLING_START_TIME]
        save_data[Master.RESPONSE_TIME] = api_crawler_document[ApiResponse.RESPONSE_TIME]

        # apiレスポンスの中身を
        save_data[Master.ISSUE_ID] = response[Master.ISSUE_ID]
        save_data[Master.IMAGE_KIND] = response[Master.IMAGE_KIND]
        save_data[Master.SEARCH_OBJECT] = response[Master.SEARCH_OBJECT]
        save_data[Master.SESSION] = response[Master.SESSION]
        save_data[Master.NAME_OF_HOUSE] = response[Master.NAME_OF_HOUSE]
        save_data[Master.NAME_OF_MEETING] = response[Master.NAME_OF_MEETING]
        save_data[Master.ISSUE] = response[Master.ISSUE]
        save_data[Master.DATE] = response[Master.DATE]
        save_data[Master.CLOSING] = response[Master.CLOSING]
        save_data[Master.SPEECH_RECORD] = response[Master.SPEECH_RECORD]
        save_data[Master.MEETING_URL] = response[Master.MEETING_URL]
        save_data[Master.PDF_URL] = response[Master.PDF_URL]
        
        # データをマスターコレクションに保存
        master_model.insert_one(save_data)

        _ = f"{response[Master.DATE]} {response[Master.NAME_OF_HOUSE]} {response[Master.NAME_OF_MEETING]} {response[Master.ISSUE]}"
        logger.info(f"マスターに保存しました: {_}")

        saved_count += 1

    logger.info(f" 結果 : 保存({saved_count})件, スキップ({skipped_count})件")        
    
