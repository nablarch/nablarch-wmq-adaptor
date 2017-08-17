# nablarch-wmq-adaptor

| master | develop |
|:-----------|:------------|
|[![Build Status](https://travis-ci.org/nablarch/nablarch-wmq-adaptor.svg?branch=master)](https://travis-ci.org/nablarch/nablarch-wmq-adaptor)|[![Build Status](https://travis-ci.org/nablarch/nablarch-wmq-adaptor.svg?branch=develop)](https://travis-ci.org/nablarch/nablarch-wmq-adaptor)|

## 依存ライブラリ

本モジュールのコンパイルまたはテストには、下記ライブラリを手動でローカルリポジトリへインストールする必要があります。

ライブラリ          |ファイル名       |グループID     |アーティファクトID   |バージョン   |
:-------------------|:----------------|:--------------|:--------------------|:------------|
WebSphere MQ ライブラリ|com.ibm.mq.commonservices.jar|com.ibm|com.ibm.mq.commonservices|7.5
WebSphere MQ ライブラリ|com.ibm.mq.headers.jar|com.ibm|com.ibm.mq.headers|7.5
WebSphere MQ ライブラリ|com.ibm.mq.jmqi.jar|com.ibm|com.ibm.mq.jmqi|7.5
WebSphere MQ ライブラリ|com.ibm.mq.pcf.jar|com.ibm|com.ibm.mq.pcf|7.5
WebSphere MQ ライブラリ|com.ibm.mq.jar|com.ibm|com.ibm.mq|7.5|
WebSphere MQ ライブラリ|connector.jar|javax.resource|connector|1.0|

上記ライブラリは、下記コマンドでインストールしてください。


```
mvn install:install-file -Dfile=<ファイル名> -DgroupId=<グループID> -DartifactId=<アーティファクトID> -Dversion=<バージョン> -Dpackaging=jar
```

- WebSphere MQ ライブラリ  
[ここ](https://www.ibm.com/developerworks/community/blogs/messaging/entry/develop_on_websphere_mq_advanced_at_no_charge?lang=ja)から開発者用WebSphere MQ v7.5をダウンロード・インストールし、jarファイルを入手してください。

