from datetime import datetime
from typing import Any, Final, Optional, List
from urllib.parse import urlparse

from api_crawl.settings import TIMEZONE
from pydantic import BaseModel, Field, field_validator, ValidationInfo

#####################################################################################
# 定数 (news_crawlの引数)
# ※クラス内で定義したかったが、その場合クラス内で参照できなかった。
#   次善の策としてモジュール定数側で定義。
#####################################################################################


class ApiCrawlInputConst:
    """NewsCrawlInput用のコンスタント定義クラス"""

    DEBUG: Final[str] = "debug"
    CRAWL_POINT_NON_UPDATE: Final[str] = "crawl_point_non_update"
    CRAWLING_START_TIME: Final[str] = "crawling_start_time"
    LASTMOD_TERM_MINUTES_FROM: Final[str] = "lastmod_term_minutes_from"
    LASTMOD_TERM_MINUTES_TO: Final[str] = "lastmod_term_minutes_to"
    PAGE_SPAN_FROM: Final[str] = "page_span_from"
    PAGE_SPAN_TO: Final[str] = "page_span_to"
    CONTINUED: Final[str] = "continued"
    DIRECT_CRAWL_URLS: Final[str] = "direct_crawl_urls"
    URL_PATTERN: Final[str] = "url_pattern"


class ApiCrawlInput(BaseModel):
    """
    News Crawlに対する引数用モデル
    引数のチェック及びデータモデルとしての機能を提供する。
    """

    # Api Crawlerの動作モードに関する引数
    debug: bool = Field(False, title="デバックモードフラグ")
    crawl_point_non_update: bool = Field(False, title="クロールポイント更新なしフラグ")

    # クロール開始となる基準時間。指定がなかった場合、現在時刻とする。
    crawling_start_time: datetime = Field(
        datetime.now().astimezone(TIMEZONE), title="クロール開始時間"
    )
    
    # # クロール対象・範囲を指定する任意引数
    start_date: str = Field(..., title="開始日")
    end_date: str = Field(..., title="終了日")

    def __init__(self, **data: Any):
        super().__init__(**data)

    """
    クラス変数側の定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    値がNoneの場合、以下のチェックは動かない。Noneでも動かす場合、「always=True」指定で動かすことができる。例）@validator('aaa', always=True)
    通常上記の型チェックが先に動く。型チェックの前に動かすには「pre=True」指定で動かすことができる。例）@validator('aaa', pre=True, always=True)
    """

    ##################################
    # 単項目チェック
    ##################################
    # @field_validator(ApiCrawlInputConst.DIRECT_CRAWL_URLS, mode="before")
    # def start_time_check(cls, value: Optional[List[str]]) -> Optional[List[str]]:
    #     if value:
    #         for url in value:
    #             parsed_url = urlparse(url)
    #             if not parsed_url.scheme:
    #                 raise ValueError(
    #                     f"引数エラー({ApiCrawlInputConst.DIRECT_CRAWL_URLS}): URLとして解析できませんでした {url}"
    #                 )
    #     return value

    # @field_validator(ApiCrawlInputConst.LASTMOD_TERM_MINUTES_TO, mode="before")
    # def lastmod_term_minutes_to_check(cls, value: Optional[int], info: ValidationInfo) -> Optional[int]:
    #     lastmod_term_minutes_from = info.data.get(ApiCrawlInputConst.LASTMOD_TERM_MINUTES_FROM)
    #     if value is not None and lastmod_term_minutes_from is not None:
    #         if value > lastmod_term_minutes_from:
    #             raise ValueError(
    #                 f"引数エラー : {ApiCrawlInputConst.LASTMOD_TERM_MINUTES_FROM} と {ApiCrawlInputConst.LASTMOD_TERM_MINUTES_TO} は、from > toで指定してください。"
    #                 f"from({lastmod_term_minutes_from}) : to({value})）"
    #             )
    #     return value

    # @field_validator(ApiCrawlInputConst.PAGE_SPAN_TO, mode="before")
    # def page_span_to_check(cls, value: Optional[int], info: ValidationInfo) -> Optional[int]:
    #     page_span_from = info.data.get(ApiCrawlInputConst.PAGE_SPAN_FROM)
    #     if (page_span_from is None) != (value is None):
    #         raise ValueError(
    #             f"引数エラー : {ApiCrawlInputConst.PAGE_SPAN_FROM} と {ApiCrawlInputConst.PAGE_SPAN_TO} は同時に指定してください。"
    #         )
    #     if value is not None and page_span_from is not None and value < page_span_from:
    #         raise ValueError(
    #             f"引数エラー : {ApiCrawlInputConst.PAGE_SPAN_FROM}と{ApiCrawlInputConst.PAGE_SPAN_TO}はfrom ≦ toで指定してください。"
    #             f"from({page_span_from}) : to({value})）"
    #         )
    #     return value

    ###################################
    #
    ###################################

if __name__ == "__main__":
    params = dict(
        crawling_start_time=datetime(2025, 4, 2, 14, 0, 0).astimezone(TIMEZONE),
        debug=True,
        crawl_point_non_update=False,
        start_date="2025-02-26",
        end_date="2025-02-26",
    )
    a = ApiCrawlInput(**params)

    print(a.debug)
    print(a.crawl_point_non_update)
    if a.crawling_start_time:
        aa: datetime = a.crawling_start_time
        print(aa)

    print("=====")

    b = ApiCrawlInput(
        debug=True,
        crawl_point_non_update=False,
        start_date="2025-02-26",
        end_date="2025-02-26",
    )

    print(b.debug)
    print(b.crawl_point_non_update)

    if b.crawling_start_time:
        bb: datetime = b.crawling_start_time
        print(bb)

    print(b.__dict__)  # クラス変数一括取得
