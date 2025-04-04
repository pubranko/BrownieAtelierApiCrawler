import sys
from scrapy.cmdline import execute
from scrapy.utils.project import get_project_settings
import traceback

args = sys.argv
# args

# execute(argv=['scrapy','crawl', args[1]])
try:
    execute(argv=args[1:])
except Exception as e:
    print("=== 例外が発生しました ===")
    print("エラー内容:", e)
    print("スタックトレース:")
    print(traceback.format_exc())