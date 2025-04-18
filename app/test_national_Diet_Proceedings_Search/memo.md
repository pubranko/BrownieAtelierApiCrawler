# 国会会議録検索

"""
国会会議録検索システム 検索用APIの仕様
https://kokkai.ndl.go.jp/api.html
apiの使用例、パラメータの種類・意味については上記参照

アクセスURL
▲会議単位簡易出力：
    https://kokkai.ndl.go.jp/api/meeting_list?{検索条件}
●会議単位出力：
    https://kokkai.ndl.go.jp/api/meeting?{検索条件}
▲発言単位出力：
    https://kokkai.ndl.go.jp/api/speech?{検索条件}

(2) 会議単位出力では、指定した検索条件でヒットした会議録の情報（回次、院、会議名、号、開催日、ID、URL等）と、
# 当該会議録の全ての発言本文のテキストデータ（発言者等の情報を含みます。）を、
1リクエストに対し最大10件まで、XML形式又はJSON形式で返戻します。
アクセスURLは https://kokkai.ndl.go.jp/api/meeting?{検索条件} です。
本文のテキストデータが返戻される点で(1) 会議単位簡易出力と、会議録中の全ての発言が返戻される点で(3) 発言単位出力と異なります。

会議開催日が「1980年」（from=1980-01-01）（until=1980-12-31）、かつ、発言者名に「田中」又は「鈴木」が含まれている（speaker=田中 鈴木）会議録
(2) 会議単位出力
    https://kokkai.ndl.go.jp/api/meeting?from=1980-01-01&until=1980-12-31&speaker=%E7%94%B0%E4%B8%AD%20%E9%88%B4%E6%9C%A8


指定可能な検索パラメータ
1	startRecord
2	maximumRecords
3	nameOfHouse
4	nameOfMeeting
5	any
6	speaker
7	from
8	until
9	supplementAndAppendix
10	contentsAndIndex
11	searchRange
12	closing
13	speechNumber
14	speakerPosition
15	speakerGroup
16	speakerRole
17	speechID
18	issueID
19	sessionFrom
20	sessionTo
21	issueFrom
22	issueTo
23	recordPacking   戻り値の形式を指定  xml or json。省略時はxml


戻り値のサンプル
JSON形式（会議単位出力、会議単位簡易出力）

{
  "numberOfRecords": 総結果件数 ,
  "numberOfReturn": 返戻件数 ,
  "startRecord": 開始位置 ,
  "nextRecordPosition": 次開始位置 ,
  "meetingRecord":[
    {
      "issueID": 会議録ID ,
      "imageKind": イメージ種別（会議録・目次・索引・附録・追録） ,
      "searchObject": 検索対象箇所（議事冒頭・本文） ,
      "session": 国会回次 ,
      "nameOfHouse": 院名 ,
      "nameOfMeeting": 会議名 ,
      "issue": 号数 ,
      "date": 開催日付 ,
      "closing": 閉会中フラグ ,
      "speechRecord":[
        {
          "speechID": 発言ID ,
          "speechOrder": 発言番号 ,
          "speaker": 発言者名 ,
          "speakerYomi": 発言者よみ（※会議単位出力のみ） ,
          "speakerGroup": 発言者所属会派（※会議単位出力のみ） ,
          "speakerPosition": 発言者肩書き（※会議単位出力のみ） ,
          "speakerRole": 発言者役割（※会議単位出力のみ） ,
          "speech": 発言（※会議単位出力のみ） ,
          "startPage": 発言が掲載されている開始ページ（※会議単位出力のみ） ,
          "createTime": レコード登録日時（※会議単位出力のみ） ,
          "updateTime": レコード更新日時（※会議単位出力のみ） ,
          "speechURL": 発言URL ,
        },
        {
          （次の発言情報）
        }
      ],
      "meetingURL": 会議録テキスト表示画面のURL ,
      "pdfURL": 会議録PDF表示画面のURL（※存在する場合のみ） ,
    },
    {
      （次の会議録情報）
    }
  ]
}

"""
