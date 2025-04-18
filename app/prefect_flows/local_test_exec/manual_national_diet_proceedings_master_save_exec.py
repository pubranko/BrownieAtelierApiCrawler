from datetime import datetime
from prefect.testing.utilities import prefect_test_harness
from prefect_flows.flows.manual_national_diet_proceedings_master_save_flow import manual_national_diet_proceedings_master_save_flow
from shared.settings import TIMEZONE

def test_exec():
    with prefect_test_harness():

        manual_national_diet_proceedings_master_save_flow(
            target_start_time_from=datetime(2025, 4, 17, 0, 0, 0).astimezone(TIMEZONE),
            target_start_time_to  =datetime(2025, 4, 18, 23, 59, 59).astimezone(TIMEZONE),
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
