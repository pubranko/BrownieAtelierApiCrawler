import importlib
import pkgutil
import inspect
from decouple import config
from prefect import get_run_logger, task
from BrownieAtelierAnalyzer.llm_models.base_model import BaseModel

@task()
def llm_launch_task() -> BaseModel:
    """
    """
    def get_llm_model_instance(selected_model: str, logger) -> BaseModel:
        """ selected_model に応じたインスタンスを返す。

        Args:
            selected_model (str): _description_
            logger (_type_): _description_

        Raises:
            ValueError: _description_

        Returns:
            BaseModel: _description_
        """
        # まずパッケージをモジュールオブジェクトとしてimport
        package = importlib.import_module("BrownieAtelierAnalyzer.llm_models")
        
        # パッケージ配下のサブモジュールを探索
        for finder, name, ispkg in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
            module = importlib.import_module(name)
            for _, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, BaseModel) and obj is not BaseModel:
                    instance = obj(logger)
                    if getattr(instance, "model_name", None) == selected_model:
                        return instance
        raise ValueError(f"指定されたモデルは利用できません: {selected_model}")


    logger = get_run_logger()  # PrefectLogAdapter
    logger.info("llm_launch_task 開始")
    selected_model:str = str(config("BROWNIE_ATELIER_ANALYZER__SELECTED_MODEL"))
    return get_llm_model_instance(selected_model, logger)
