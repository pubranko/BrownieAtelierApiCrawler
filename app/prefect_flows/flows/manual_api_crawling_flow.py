from typing import Any

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect_flows.flows import START_TIME
from prefect_flows.flows.init_flow import init_flow
from prefect_flows.tasks.api_crawling_task import api_crawling_task
from prefect_flows.tasks.end_task import end_task
from prefect_flows.tasks.init_task import init_task
from prefect_flows.tasks.manual_api_crawling_target_spiders_task import manual_api_crawling_target_spiders_task


@flow(name="Manual api crawling flow")
def manual_api_crawling_flow(
    spider_names: list[str], spider_kwargs: dict, following_processing_execution: bool
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
            # クローラー用引数を生成、クロール対象スパイダーを生成し、クローリングを実行する。
            # news_crawl_input: NewsCrawlInput = crawling_input_create_task(spider_kwargs)
            api_crawling_target_spiders = manual_api_crawling_target_spiders_task(spider_names)
            # crawling_task(news_crawl_input, crawling_target_spiders)
            api_crawling_task(spider_kwargs, api_crawling_target_spiders)

            if following_processing_execution:
                # スクレイピング結果をニュースクリップマスターへ保存
                # news_clip_master_save_task(mongo, "", START_TIME, START_TIME)
                pass

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.exception(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")
