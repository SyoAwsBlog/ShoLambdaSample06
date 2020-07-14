module.exports = function AbstractFirehosePutWorkerCommon() {
  var Promise;

  // 継承：親クラス＝AbstractBizCommon.js
  var superClazzFunc = require("./AbstractBizCommon.js");
  AbstractFirehosePutWorkerCommon.prototype = new superClazzFunc();

  // 各ログレベルの宣言
  var LOG_LEVEL_TRACE = 1;
  var LOG_LEVEL_DEBUG = 2;
  var LOG_LEVEL_INFO = 3;
  var LOG_LEVEL_WARN = 4;
  var LOG_LEVEL_ERROR = 5;

  // 現在の出力レベルを設定(ワーカー処理は別のログレベルを設定可能とする)
  var LOG_LEVEL_CURRENT = LOG_LEVEL_INFO;
  if (process && process.env && process.env.LogLevelForWorker) {
    LOG_LEVEL_CURRENT = process.env.LogLevelForWorker;
  }

  // SQSのURL
  var SQS_QUEUE_URL = "";
  if (process && process.env && process.env.SqsQueueUrl) {
    SQS_QUEUE_URL = process.env.SqsQueueUrl;
  }

  // FirehoseのStream名
  var FIREHOSE_STREAM_NAME = "";
  if (process && process.env && process.env.FirehoseStreamName) {
    FIREHOSE_STREAM_NAME = process.env.FirehoseStreamName;
  }

  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.getSqsQueueUrl = function () {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# getSqsQueueUrl : start"
      );
      return SQS_QUEUE_URL;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# getSqsQueueUrl : end"
      );
    }
  }.bind(AbstractFirehosePutWorkerCommon); // AbstractFirehosePutWorkerCommonをthisとする

  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.getFirehoseStreamName = function () {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# getFirehoseStreamName : start"
      );
      return FIREHOSE_STREAM_NAME;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# getFirehoseStreamName : end"
      );
    }
  }.bind(AbstractFirehosePutWorkerCommon); // AbstractFirehosePutWorkerCommonをthisとする

  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.getSqsDeleteParam = function (
    record
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# getSqsDeleteParam : start"
      );

      var queueUrl = base.getSqsQueueUrl();
      var params = {
        QueueUrl: queueUrl,
        ReceiptHandle: record.ReceiptHandle,
      };
      return params;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# getSqsDeleteParam : end"
      );
    }
  }.bind(AbstractFirehosePutWorkerCommon); // AbstractFirehosePutWorkerCommonをthisとする

  /*
  現在のログレベルを返却する
  ※　処理制御側とログレベルを同一設定で行う場合は、オーバーライドをコメントアウトする
  */
  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.getLogLevelCurrent = function () {
    return LOG_LEVEL_CURRENT;
  }.bind(AbstractFirehosePutWorkerCommon.prototype);
  /*
  出力レベル毎のログ処理
  */
  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.writeLogTrace = function (
    msg
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelTrace() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype);

  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.writeLogDebug = function (
    msg
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelDebug() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype);

  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.writeLogInfo = function (
    msg
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelInfo() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype);

  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.writeLogWarn = function (
    msg
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelWarn() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype);

  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.writeLogError = function (
    msg
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelError() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype);

  // 処理の実行
  function* executeBizWorkerCommon(event, context, bizRequireObjects) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# executeBizWorkerCommon : start"
      );
      if (bizRequireObjects.PromiseObject) {
        Promise = bizRequireObjects.PromiseObject;
      }
      AbstractFirehosePutWorkerCommon.prototype.RequireObjects = bizRequireObjects;

      return yield AbstractFirehosePutWorkerCommon.prototype.executeBizCommon(
        event,
        context,
        bizRequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# executeBizWorkerCommon : end"
      );
    }
  }
  AbstractFirehosePutWorkerCommon.prototype.executeBizWorkerCommon = executeBizWorkerCommon;

  /*
  業務前処理

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.beforeMainExecute = function (
    args
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# beforeMainExecute : start"
      );

      base.writeLogInfo(
        "AbstractFirehosePutWorkerCommon# beforeMainExecute:args:" +
          JSON.stringify(args)
      );

      return Promise.resolve(args);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# beforeMainExecute : end"
      );
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon);

  /*
  メッセージの内容抽出（DynamoDB→SNS→SQS）でメッセージを受け取った前提

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.extractBizInfos = function (
    args
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# extractBizInfos : start"
      );

      return new Promise(function (resolve, reject) {
        try {
          var record = base.getLastIndexObject(args);
          base.writeLogTrace(
            "AbstractFirehosePutWorkerCommon# record" + JSON.stringify(record)
          );

          // SQSのプロパティから取り出し
          var recordBody = JSON.parse(record.Body);
          base.writeLogTrace(
            "AbstractFirehosePutWorkerCommon# recordBody" +
              JSON.stringify(recordBody)
          );

          // SNSのプロパティから取り出し
          var recordMessage = JSON.parse(recordBody.Message);
          base.writeLogTrace(
            "AbstractFirehosePutWorkerCommon# recordMessage" +
              JSON.stringify(recordMessage)
          );

          // DynamoDBのプロパティから取り出し
          var recordImage = recordMessage.NewImage;
          base.writeLogTrace(
            "AbstractFirehosePutWorkerCommon# recordImage" +
              JSON.stringify(recordImage)
          );

          resolve(recordImage);
        } catch (err) {
          reject(err);
        }
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# extractBizInfos : end"
      );
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon);

  /*
  出力項目のエスケープ処理と連想配列を取り扱いやすい構造に変換

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.transformRecordInfos = function (
    args
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# transformRecordInfos : start"
      );

      // 直前の処理結果を取得
      var record = base.getLastIndexObject(args);

      return new Promise(function (resolve, reject) {
        try {
          var dest = {};
          for (var key in record) {
            var val = null;
            var valMap = record[key];
            for (var valKey in valMap) {
              val = valMap[valKey];
            }
            dest[key] = val;
            base.writeLogTrace(
              "AbstractFirehosePutWorkerCommon# key:" + key + "# val:" + val
            );
          }
          resolve(dest);
        } catch (err) {
          base.printStackTrace(err);
          reject(err);
        }
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# transformRecordInfos : end"
      );
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon);

  /*
  Firehoseへの出力カラムをDynamoDB項目名の配列で返却する

  */
  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.getFileOutputColumns = function (
    record
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# getFileOutputColumns : start"
      );
      var cols = [];
      return cols;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# getFileOutputColumns : end"
      );
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon);

  /*
  Firehoseへの出力行データを生成する（サンプルはCSVデータ化）

  @param record 引数のSQSメッセージを構造化したデータ
  */
  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.getFileOutputLayout = function (
    record
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# getFileOutputLayout : start"
      );

      var recordData = "";
      var cols = base.getFileOutputColumns();

      for (var i = 0; i < cols.length; i++) {
        var colName = cols[i];

        if (i > 0) {
          recordData = recordData + ",";
        }

        if (
          colName in record &&
          record[colName] &&
          record[colName].length > 0
        ) {
          recordData = recordData + "" + record[colName];
        } else {
          recordData = recordData + "";
        }
      }

      recordData = recordData + "\n";

      return recordData;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# getFileOutputLayout : end"
      );
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon);

  /*
  業務メイン処理

  Firehoseにputを実行する

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.businessMainExecute = function (
    args
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# businessMainExecute : start"
      );

      var record = base.getLastIndexObject(args);

      var streamName = base.getFirehoseStreamName();
      var fileRecordData = base.getFileOutputLayout(record);

      var params = {
        DeliveryStreamName: streamName,
        Record: { Data: fileRecordData },
      };

      return new Promise(function (resolve, reject) {
        var firehoseCallback = function (err, data) {
          if (err) {
            base.printStackTrace(err);
            base.writeLogWarn(
              "AbstractFirehosePutWorkerCommon# Firehose Put Retry"
            );
            base.RequireObjects.Firehose.putRecord(params, firehoseCallback);
          } else {
            base.writeLogTrace(
              "AbstractFirehosePutWorkerCommon# Firehose Response :" +
                JSON.stringify(data)
            );
            resolve(data);
          }
        };
        base.RequireObjects.Firehose.putRecord(params, firehoseCallback);
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# businessMainExecute : end"
      );
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon);

  /*
  業務前処理

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.afterMainExecute = function (
    args
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# afterMainExecute : start"
      );

      var record = base.getFirstIndexObject(args);

      return new Promise(function (resolve, reject) {
        try {
          var params = base.getSqsDeleteParam(record);
          base.writeLogTrace(
            "AbstractFirehosePutWorkerCommon# SQS DeleteParam :" +
              JSON.stringify(params)
          );

          base.RequireObjects.Sqs.deleteMessage(params, function (err, data) {
            if (err) {
              base.printStackTrace(err);
              reject(err);
            } else {
              base.writeLogTrace(
                "AbstractFirehosePutWorkerCommon# SQS Response :" +
                  JSON.stringify(data)
              );
              resolve(data);
            }
          });
        } catch (err) {
          base.printStackTrace(err);
          reject(err);
        }
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractFirehosePutWorkerCommon# afterMainExecute : end"
      );
    }
  }.bind(AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon);

  /*
  直列処理のメソッド配列を返却する(オーバーライド)

  サンプル用にログ出力量を減らす為、後処理を間引く

  */
  AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon.getTasks = function (
    event,
    context
  ) {
    var base = AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("AbstractFirehosePutWorkerCommon# getTasks : start");
      return [
        this.beforeMainExecute,
        this.extractBizInfos,
        this.transformRecordInfos,
        this.businessMainExecute,
        this.afterMainExecute,
      ];
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractFirehosePutWorkerCommon# getTasks : end");
    }
  };

  // 関数定義は　return　より上部に記述
  // 外部から実行できる関数をreturnすること
  return {
    executeBizWorkerCommon,
    AbstractBaseCommon:
      AbstractFirehosePutWorkerCommon.prototype.AbstractBaseCommon,
    AbstractBizCommon: AbstractFirehosePutWorkerCommon.prototype,
  };
};
