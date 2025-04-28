from typing import Any, Generator, Optional
from datetime import datetime, date, time
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.national_diet_proceedings_master_model import NationalDietProceedingsMasterModel
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect_flows.flows import START_TIME
from prefect_flows.flows.init_flow import init_flow
from prefect_flows.tasks.meeting_summary_key_list_create_task import meeting_summary_key_list_create_task
from prefect_flows.tasks.end_task import end_task
from prefect_flows.tasks.init_task import init_task
from prefect_flows.tasks.llm_launch_task import llm_launch_task
from BrownieAtelierAnalyzer.llm_models.mistral.mistral_large_latest import MistralLargeLatest

# from prefect_flows.tasks.meeting_data_get import meeting_data_get
from pymongo import ASCENDING, DESCENDING
from pymongo.cursor import Cursor
from time import sleep
import re


@flow(name="Manual meeting summary flow")
def manual_meeting_summary_flow(
    target_crawling_start_time_from: Optional[datetime],
    target_crawling_start_time_to: Optional[datetime],
    target_meeting_date_from: Optional[date],
    target_meeting_date_to: Optional[date],
):
    init_flow()

    # ロガー取得
    logger = get_run_logger()  # PrefectLogAdapter
    # 初期処理
    init_task_instance: PrefectFuture = init_task.submit()
    # 実行結果が返ってくるまで待機し、戻り値を保存。 
    #   ※タスクのステータスをresultを受け取る前に判定してもPendingとなる。インスタンスのステータスはリアルタイムで更新されているので注意。
    init_task_result = init_task_instance.result()

    if init_task_instance.state.is_completed():
        mongo: MongoModel = init_task_result

        try:

            master_filter_keys: list[dict] = meeting_summary_key_list_create_task(
                mongo,
                target_crawling_start_time_from,
                target_crawling_start_time_to,
                target_meeting_date_from,
                target_meeting_date_to,
                )

            master = NationalDietProceedingsMasterModel(mongo)
            sort_parameter = {
                master.NAME_OF_HOUSE: ASCENDING,
                master.NAME_OF_MEETING: ASCENDING,
                master.ISSUE: ASCENDING,
            }
            projection = {
                master._ID: 0,
                master.CRAWLING_START_TIME:0,
                master.RESPONSE_TIME:0,
            }

            master_record_base_items :dict = {}
            master_record_by_key: list = []
            # LLMを起動
            llm: MistralLargeLatest = llm_launch_task()

            # print(llm.model_infomation())

            # 同一の会議単位で繰り返し（nameOfHouse nameOfMeeting issue）
            for master_filter in master_filter_keys:

                filter_2 = {"$and": [master_filter["_id"]]}

                # 同一KEYのマスターを取得
                master_records: Cursor = master.find(
                    projection = projection, 
                    filter = filter_2, 
                    sort=sort_parameter)

                for master_record in master_records:
                    master_record: dict
                    logger.info(f"会議録URL: {master_record[master.MEETING_URL]}")

                    # print(record[master.SPEECH_RECORD][0]["speech"])
                    # 半角スペースまたは全角スペースが2つ以上連続している部分を半角スペース１つに置き換え
                    
                    # record[master.SPEECH_RECORD] = [
                    #     re.sub(r'[ \u3000]{2,}', ' ', str(speech_record)).replace('\n', '')
                    #     for speech_record in record[master.SPEECH_RECORD]
                    # ]
                    
                    # # LLMで要約時に不要な項目を削除
                    # del master_record[master._ID]
                    # del master_record[master.CRAWLING_START_TIME]
                    # del master_record[master.RESPONSE_TIME]
                    
                    for speech_record_dict in master_record[master.SPEECH_RECORD]:
                        speech_record_dict:dict
                        # speech_record_dict["speech"] = re.sub(r'[ \u3000]{2,}', ' ', speech_record_dict["speech"]).replace('\n', '')
                        speech_record_dict["speech"] = re.sub(r'[ \u3000]{2,}', ' ', speech_record_dict["speech"]).replace('\r\n', '\n')
                        del speech_record_dict["startPage"]
                        del speech_record_dict["createTime"]
                        del speech_record_dict["updateTime"]
                        del speech_record_dict["speechURL"]
                        master_record[master.SPEECH_RECORD].append(speech_record_dict)
                    
                    # print(record[master.SPEECH_RECORD][0]["speech"])
                    
                    # 置き換え後の結果をリストに保存
                    master_record_by_key.append(master_record)
                    master_record_base_items
                    
                # print(master_record_by_key)
                pass

            
                # プロンプトを作成　（1.会議録の要約）
                # prompt_1 = f"""
                #     国会会議録検索システムより取得した以下のデータを要約してください。
                #     ○根本委員長　次に、合同審査会開会に関する件についてお諮りいたします。\r\n　国家の基本政策に関する件について、本会期中、参議院国家基本政策委員会と合同審査会を開会いたしたいと存じますが、御異議ありませんか。\r\n　　　　〔「異議なし」と呼ぶ者あり〕
                # """
                # プロンプトを作成　（2.Q＆A形式）
                # prompt_2 = prompt_create("")
                # プロンプトを作成　（3.アクションアイテム形式）
                # prompt_3 = prompt_create("")
                # プロンプトを作成　（4.ストーリーテリング形式）
                # prompt_4 = prompt_create("")

                # prompt_1 = 

                
                # llm.chat(prompt = master_record_by_key)
                # # print(llm.chat_response_to_text())
                # response_text: str = llm.chat_response_to_text()
                # print(response_text)
                
                # llm.usage_info()

                
                # 各プロンプトをLLMに与え結果をmongoDBへ保存する。
                # save_mieeting_summary(mongo, llm, prompt_1)
                # save_mieeting_summary(mongo, llm, prompt_2)
                # save_mieeting_summary(mongo, llm, prompt_3)
                # save_mieeting_summary(mongo, llm, prompt_4)

                # 連続のリクエストには最低１秒間を開ける必要がある。
                # misttalの制限事項
                #   https://help.mistral.ai/en/articles/225174-what-are-the-limits-of-the-free-tier
                #   ・1秒あたり1リクエスト 
                #   ・1分あたり50万トークン 
                #   ・毎月10億トークン
                sleep(1)

            # meeting_summary_task(mongo)


        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.exception(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")
