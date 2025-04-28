import logging
from logging import FileHandler, StreamHandler
from typing import Any, Optional
from datetime import datetime,date,time
from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE
from BrownieAtelierAnalyzer.llm_models.mistral.mistral_large_latest import MistralLargeLatest
from shared.settings import TIMEZONE
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.national_diet_proceedings_master_model import NationalDietProceedingsMasterModel
from pymongo import ASCENDING, DESCENDING
from pymongo.cursor import Cursor

@task(cache_policy=NO_CACHE)    # cache_policy=NO_CACHE タスクのキャッシュ機能を無効化。内部でシリアライズできないkey(mongo)があるとエラーとなるため。
def meeting_summary_key_list_create_task(
    mongo: MongoModel,
    target_crawling_start_time_from: Optional[datetime],
    target_crawling_start_time_to: Optional[datetime],
    target_meeting_date_from: Optional[date],
    target_meeting_date_to: Optional[date],
) -> list[dict]:
    """
    """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info("meeting_summary_key_list_create_task 開始")

    # まず会議録を取得するタスクを追加
    # meeting_data_get(mongo, target_meeting_date)
    """
    national_diet_proceedings_masterより crawling_start_time で対象データを抽出(projectionでkey項目のみ)。
    key項目 (nameOfHouse, nameOfMeeting) を使い national_diet_proceedings_master よりデータを取得。
    ※同一会議が複数日に跨っている場合がある。
    ※取得時不要項目は除外 (_id,crawling_start_time,response_time,,,)
    同一会議毎に1つのragとしてまとめたリストを生成。
    """
    master = NationalDietProceedingsMasterModel(mongo)

    target_crawling_start_time_conditions = {}
    if target_crawling_start_time_from:
        target_crawling_start_time_conditions["$gte"] = target_crawling_start_time_from
    if target_crawling_start_time_to:
        target_crawling_start_time_conditions["$lte"] = target_crawling_start_time_to

    target_meeting_date_conditions = {}
    if target_meeting_date_from:
        target_meeting_date_conditions["$gte"] = target_meeting_date_from.strftime("%Y-%m-%d")
    if target_meeting_date_to:
        target_meeting_date_conditions["$lte"] = target_meeting_date_to.strftime("%Y-%m-%d")

    _ = []
    if target_crawling_start_time_conditions:
        _.append({master.CRAWLING_START_TIME: target_crawling_start_time_conditions})
    if target_meeting_date_conditions:
        _.append({master.DATE: target_meeting_date_conditions})
    filter_1 = {"$and": _}

    target_crawling_start_time_conditions = master.count(filter = filter_1)
    if target_crawling_start_time_conditions <= 0:
        logger.info("対象の国会会議録が存在しません。Flowの実行を停止します。")
        return []
    else:
        logger.info(f"対象データ件数 {target_crawling_start_time_conditions}件")

    sort_parameter = {
        master.NAME_OF_HOUSE: ASCENDING,
        master.NAME_OF_MEETING: ASCENDING,
    }
    
    # master_filter_keys:list[dict] = list(master.aggregate(
    return list(master.aggregate(
        aggregate_items={
            master.NAME_OF_HOUSE: f"${master.NAME_OF_HOUSE}",
            master.NAME_OF_MEETING: f"${master.NAME_OF_MEETING}",
        },
        filter=filter_1,
        sort=sort_parameter,
    ))
