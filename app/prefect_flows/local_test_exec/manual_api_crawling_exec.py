import glob
import os
from prefect.testing.utilities import prefect_test_harness
from prefect_flows.flows.manual_api_crawling_flow import manual_api_crawling_flow


def test_exec():
    with prefect_test_harness():

        manual_api_crawling_flow(
            spider_names=[
                "national_diet_proceedings",
            ],
            spider_kwargs=dict(
                start_date="2025-02-26",
                end_date="2025-02-27",
                debug=True,
                # continued = True,
                # crawl_point_non_update = True,
            ),
            # following_processing_execution=False    # 後続処理実行(scrapying,news_clip_masterへの登録,solrへの登録)
            following_processing_execution=True  # 後続処理実行(scrapying,news_clip_masterへの登録,solrへの登録)
        )

if __name__ == "__main__":
    test_exec()

"""
{
    "spider_names": [
        "national_diet_proceedings",
        "asahi_com_sitemap",
        "kyodo_co_jp_sitemap",
        "yomiuri_co_jp_sitemap",
        "jp_reuters_com_sitemap",
        "epochtimes_jp_crawl",
        "mainichi_jp_crawl",
        "nikkei_com_crawl"
    ],

    "spider_kwargs": {
        "debug": true,
        "page_span_from": 2,
        "page_span_to": 2,
        "start_date": "2025-02-26",
        "end_date": "2025-02-27",
    },

    "following_processing_execution": true
}
"""
