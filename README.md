#概要

Spark StreamingでTwitterから発言を感情解析し集計

##事前に準備するもの
Twitterの開発者アカウント

### 設定ファイル

設定ファイルのサンプルをコピーし値を記述

``cp config/application.properties.sample config/application.properties``

ユーザー辞書を使用しない場合は./dictionary/blank.txtを指定する

## 起動

``sbt``

でSBTコンソールに入って

``run``

