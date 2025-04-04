
# 必要なサブモジュール
git submodule add https://github.com/pubranko/BrownieAtelierMongo.git
git submodule add https://github.com/pubranko/BrownieAtelierStorage.git
git submodule add https://github.com/pubranko/BrownieAtelierNotice.git


# プロジェクトの作成
scrapy startproject api_crawl .

# スパイダーの作成
scrapy genspider national_diet_proceedings kokkai.ndl.go.jp

# github copilotお試し
## コパイロットの編集 -> アクティブなファイルに直接更新を行っている。
- scrapyのspiderを使って、国会会議録をクロールさせたい。　引数をつかって日時指定で対称を絞り込める機能が必要。
- Scrapyのspiderを自動作成してくれる。
- pydanticの@validatorが非推奨となっている問題が発生。github-copilotに直してと依頼したら直してくれた。
  ただ精度は完全ではなかった。vscodeに上がったエラーを渡して再度修正を依頼したところエラーが解消された。

# prefectプロジェクト作成
news_crawl側ソースを流用するためgithub-actionsは使えない、、、

