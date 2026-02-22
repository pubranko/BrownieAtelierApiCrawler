from typing import Optional
from datetime import datetime, date
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect_flows.flows.init_flow import init_flow
from prefect_flows.tasks.meeting_summary_key_list_create_task import meeting_summary_key_list_create_task
from prefect_flows.tasks.meeting_summary_task import meeting_summary_task
from prefect_flows.tasks.end_task import end_task
from prefect_flows.tasks.init_task import init_task
from prefect_flows.tasks.llm_launch_task import llm_launch_task
from BrownieAtelierAnalyzer.llm_models.base_model import BaseModel
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel


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

            # LLMを起動
            llm: BaseModel = llm_launch_task()

            meeting_summary_task(mongo, master_filter_keys, llm)


            # #######################################
            # master = NationalDietProceedingsMasterModel(mongo)
            # sort_parameter = {
            #     master.NAME_OF_HOUSE: ASCENDING,
            #     master.NAME_OF_MEETING: ASCENDING,
            #     master.ISSUE: ASCENDING,
            # }
            # projection = {
            #     master._ID: 0,
            #     master.CRAWLING_START_TIME:0,
            #     master.RESPONSE_TIME:0,
            # }

            # master_record_base_items :dict = {}
            # master_record_by_key: list = []

            # # print(llm.model_infomation())

            # # 同一の会議単位(nameOfHouse nameOfMeeting)で、LLMに問い合わせるプロンプトを生成する。
            # for master_filter in master_filter_keys:

            #     filter_2 = {"$and": [master_filter["_id"]]}

            #     # 同一KEYのマスターを取得
            #     master_records: Cursor = master.find(
            #         projection = projection, 
            #         filter = filter_2, 
            #         sort=sort_parameter)

            #     for master_record in master_records:
            #         master_record: dict

            #         logger.info(f"会議録URL: {master_record[master.MEETING_URL]}")

            #         # 置き換え後の結果をリストに保存
            #         master_record_by_key.append(master_record)

            #         # 院名、会議名
            #         master_record_base_items[master.NAME_OF_HOUSE] = master_record[master.NAME_OF_HOUSE]
            #         master_record_base_items[master.NAME_OF_MEETING] = master_record[master.NAME_OF_MEETING]

            #         # 号数
            #         if master.ISSUE in master_record_base_items:
            #             master_record_base_items[master.ISSUE].append(master_record[master.ISSUE])
            #         else:
            #             master_record_base_items[master.DATE] = [master_record[master.DATE]]

            #         # 会議日
            #         if master.DATE in master_record_base_items:
            #             master_record_base_items[master.DATE].append(master_record[master.DATE])
            #         else:
            #             master_record_base_items[master.DATE] = [master_record[master.DATE]]

            #         # ミーティングURL
            #         if master.MEETING_URL in master_record_base_items:
            #             master_record_base_items[master.MEETING_URL].append(master_record[master.MEETING_URL])
            #         else:
            #             master_record_base_items[master.MEETING_URL] = [master_record[master.MEETING_URL]]

            #         # スピーチレコードより不要データを削除したデータを生成
            #         custom_speech_records: list = []
            #         for speech_record_dict in master_record[master.SPEECH_RECORD]:
            #             speech_record_dict:dict
            #             # speech_record_dict["speech"] = re.sub(r'[ \u3000]{2,}', ' ', speech_record_dict["speech"]).replace('\n', '')
            #             speech_record_dict["speech"] = re.sub(r'[ \u3000]{2,}', ' ', speech_record_dict["speech"]).replace('\r\n', '\n')
            #             del speech_record_dict["startPage"]
            #             del speech_record_dict["createTime"]
            #             del speech_record_dict["updateTime"]
            #             del speech_record_dict["speechURL"]
                        
            #             custom_speech_records.append(speech_record_dict)
            #             # master_record[master.SPEECH_RECORD].append(speech_record_dict)
                    
            #         master_record_base_items[master.SPEECH_RECORD] = custom_speech_records

            #     # プロンプトのおおよそのトークン数を計算
            #     enc = tiktoken.get_encoding("gpt2")
            #     # prompt_1 = f"""国会会議録検索システムより取得した以下のデータを要約してください。
            #     prompt_1 = f"""国会会議録検索システムより取得した以下のデータより、論点を列挙した一覧がほしい。
            #     話し言葉は使わずできるだけ簡潔に。
                
            #     {master_record_base_items}
            #     """
            #     num_tokens = len(enc.encode(prompt_1))
            #     print(num_tokens)
                
            #     llm.chat(prompt = prompt_1)
            #     response_text: str = llm.chat_response_to_text()
            #     print(response_text)

            
            #     # プロンプトを作成　（1.会議録の要約）
            #     # prompt_1 = f"""
            #     #     国会会議録検索システムより取得した以下のデータを要約してください。
            #     #     ○根本委員長　次に、合同審査会開会に関する件についてお諮りいたします。\r\n　国家の基本政策に関する件について、本会期中、参議院国家基本政策委員会と合同審査会を開会いたしたいと存じますが、御異議ありませんか。\r\n　　　　〔「異議なし」と呼ぶ者あり〕
            #     # """
            #     # プロンプトを作成　（2.Q＆A形式）
            #     # prompt_2 = prompt_create("")
            #     # プロンプトを作成　（3.アクションアイテム形式）
            #     # prompt_3 = prompt_create("")
            #     # プロンプトを作成　（4.ストーリーテリング形式）
            #     # prompt_4 = prompt_create("")

            #     # prompt_1 = 

                
            #     # llm.chat(prompt = master_record_by_key)
            #     # # print(llm.chat_response_to_text())
            #     # response_text: str = llm.chat_response_to_text()
            #     # print(response_text)
                
            #     # llm.usage_info()

                
            #     # 各プロンプトをLLMに与え結果をmongoDBへ保存する。
            #     # save_mieeting_summary(mongo, llm, prompt_1)
            #     # save_mieeting_summary(mongo, llm, prompt_2)
            #     # save_mieeting_summary(mongo, llm, prompt_3)
            #     # save_mieeting_summary(mongo, llm, prompt_4)

            #     # 連続のリクエストには最低１秒間を開ける必要がある。
            #     # misttalの制限事項
            #     #   https://help.mistral.ai/en/articles/225174-what-are-the-limits-of-the-free-tier
            #     #   ・1秒あたり1リクエスト 
            #     #   ・1分あたり50万トークン 
            #     #   ・毎月10億トークン
            #     sleep(1)

            # # meeting_summary_task(mongo)


        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.exception(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")
