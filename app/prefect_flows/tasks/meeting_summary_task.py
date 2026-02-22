import re
import tiktoken
from copy import deepcopy
from time import sleep
from prefect import task, get_run_logger
from pymongo import ASCENDING
from pymongo.cursor import Cursor
from prefect.cache_policies import NO_CACHE
from BrownieAtelierAnalyzer.llm_models.base_model import BaseModel
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.national_diet_proceedings_master_model import NationalDietProceedingsMasterModel


@task(cache_policy=NO_CACHE)    # cache_policy=NO_CACHE タスクのキャッシュ機能を無効化。内部でシリアライズできないkey(mongo)があるとエラーとなるため。
def meeting_summary_task(
    mongo: MongoModel,
    master_filter_keys: list[dict],
    llm: BaseModel,
):
    """
    """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info("meeting_summary_task 開始")


    master = NationalDietProceedingsMasterModel(mongo)
    # 同一会議体毎、実施日毎にソート
    sort_parameter = {
        master.NAME_OF_HOUSE: ASCENDING,
        master.NAME_OF_MEETING: ASCENDING,
        master.ISSUE: ASCENDING,
    }
    # _id、CRAWLING_START_TIME、RESPONSE_TIMEを除外
    projection = {
        master._ID: 0,
        master.CRAWLING_START_TIME:0,
        master.RESPONSE_TIME:0,
    }

    # master_record_base_items :dict = {}
    master_record_by_key: list = []

    # print(llm.model_infomation())

    # 同一の会議単位(nameOfHouse nameOfMeeting)で、LLMに問い合わせるプロンプトを生成する。
    for master_filter in master_filter_keys:

        filter_2 = {"$and": [master_filter["_id"]]}

        # 同一KEYのマスターを取得
        master_records: Cursor = master.find(
            projection = projection, 
            filter = filter_2, 
            sort=sort_parameter)

        # 同一の会議体のレコード毎に処理を実施
        master_record_base_items :dict = {}
        for master_record in master_records:
            master_record: dict
            

            logger.info(f"会議録URL: {master_record[master.MEETING_URL]}")

            # 置き換え後の結果をリストに保存
            master_record_by_key.append(deepcopy(master_record))

            # 院名、会議名を保存
            master_record_base_items[master.NAME_OF_HOUSE] = master_record[master.NAME_OF_HOUSE]
            master_record_base_items[master.NAME_OF_MEETING] = master_record[master.NAME_OF_MEETING]

            # 号数を配列にして保存
            if master.ISSUE in master_record_base_items:
                master_record_base_items[master.ISSUE].append(master_record[master.ISSUE])
            else:
                master_record_base_items[master.DATE] = [master_record[master.DATE]]

            # 会議日を配列にして保存
            if master.DATE in master_record_base_items:
                master_record_base_items[master.DATE].append(master_record[master.DATE])
            else:
                master_record_base_items[master.DATE] = [master_record[master.DATE]]

            # ミーティングURLを配列にして保存
            if master.MEETING_URL in master_record_base_items:
                master_record_base_items[master.MEETING_URL].append(master_record[master.MEETING_URL])
            else:
                master_record_base_items[master.MEETING_URL] = [master_record[master.MEETING_URL]]

            # スピーチレコードより不要データを削除したデータを生成
            custom_speech_records: list = []
            for speech_record_dict in master_record[master.SPEECH_RECORD]:
                speech_record_dict:dict
                # speech_record_dict["speech"] = re.sub(r'[ \u3000]{2,}', ' ', speech_record_dict["speech"]).replace('\n', '')
                speech_record_dict["speech"] = re.sub(r'[ \u3000]{2,}', ' ', speech_record_dict["speech"]).replace('\r\n', '\n')
                del speech_record_dict["startPage"]
                del speech_record_dict["createTime"]
                del speech_record_dict["updateTime"]
                del speech_record_dict["speechURL"]
                
                custom_speech_records.append(speech_record_dict)
                # master_record[master.SPEECH_RECORD].append(speech_record_dict)
            
            # master_record_base_items[master.SPEECH_RECORD] = custom_speech_records
            # ミーティングURL
            if master.SPEECH_RECORD in master_record_base_items:
                master_record_base_items[master.SPEECH_RECORD].append(custom_speech_records)
            else:
                master_record_base_items[master.SPEECH_RECORD] = custom_speech_records


        # プロンプトのおおよそのトークン数を計算
        enc = tiktoken.get_encoding("gpt2")
        # num_tokens: int = len(enc.encode(f"{master_record_base_items}"))
        num_tokens: int = len(enc.encode(f"{master_record_base_items}"))
        logger.info(f"トークン数推定: {num_tokens} 件")

        # 各レコード毎のトークン数を計算
        # token_counts: list = []
        # for items in master_record_base_items.values():
        #     token_counts.append(len(enc.encode(f"{items}")))
        # print(f"レコード毎のトークン数推定: {token_counts}")
            

        # if num_tokens <= 100000:
        # prompt_1 = f"""国会会議録検索システムより取得した以下のデータを要約してください。
        prompt_1 = f"""国会会議録検索システムより取得した以下のデータより、論点を列挙した一覧がほしい。
        話し言葉は使わずできるだけ簡潔に。
        プロンプトの内容を最初に復唱は不要。
        回答はyaml形式で出力すること。
        ナンバリングは不要。
        論点の区切りは改行コードとする。
        {master_record}
        """
    
        llm.chat(prompt = prompt_1)
        response_text: str = llm.chat_response_to_text()
        logger.info(response_text)


        # 連続のリクエストには最低１秒間を開ける必要がある。
        # misttalの制限事項
        #   https://help.mistral.ai/en/articles/225174-what-are-the-limits-of-the-free-tier
        #   ・1秒あたり1リクエスト 
        #   ・1分あたり50万トークン 
        #   ・毎月10億トークン
        sleep(1)
