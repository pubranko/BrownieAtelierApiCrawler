import logging
from typing import Any, Optional
from datetime import datetime,date,time
from prefect import get_run_logger, task
# from llm_models.rinna.japanese_gpt_1b import JapaneseGPT1B
from BrownieAtelierAnalyzer.llm_models.mistral.mistral_large_latest import MistralLargeLatest
from shared.settings import TIMEZONE

@task()
def llm_launch_task():
    """
    """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info("llm_launch_task 開始")

    return MistralLargeLatest(logger)
    
