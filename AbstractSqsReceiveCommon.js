/*
業務毎（機能枚）に共通処理な処理などを、この層に実装する。

例えば
・API Gateway から呼び出すLambdaに共通
・SQSを呼び出すLambdaに共通
など、用途毎に共通となるような処理は、この層に実装すると良い

written by syo
http://awsblog.physalisgp02.com
*/
module.exports = function AbstractSqsReceiveCommon() {
  // 疑似的な継承関係として親モジュールを読み込む
  var superClazzFunc = require("./AbstractBizCommon");
  // prototypeにセットする事で継承関係のように挙動させる
  AbstractSqsReceiveCommon.prototype = new superClazzFunc();

  // ワーカー処理同時実行数
  var EXECUTORS_THREADS_NUM = "1";
  if (process && process.env && process.env.ExecutorsThreadsNum) {
    EXECUTORS_THREADS_NUM = process.env.ExecutorsThreadsNum;
  }

  // ワーカー処理トランザクション制御（１周毎にwaitする：ミリ秒指定）
  var EXECUTORS_THREADS_WAIT = "0";
  if (process && process.env && process.env.ExecutorsThreadsWait) {
    EXECUTORS_THREADS_WAIT = process.env.ExecutorsThreadsWait;
  }

  // SQSから受信するメッセージ最大数
  var SQS_RECEIVE_MAX_SIZE = "50";
  if (process && process.env && process.env.SqsReceiveMaxSize) {
    SQS_RECEIVE_MAX_SIZE = process.env.SqsReceiveMaxSize;
  }

  // SQSから受信する際のエラーリトライ上限
  var SQS_RECEIVE_ERR_RETRY_MAX = "5";
  if (process && process.env && process.env.SqsReceiveErrRetryMax) {
    SQS_RECEIVE_ERR_RETRY_MAX = process.env.SqsReceiveErrRetryMax;
  }

  // SQSのURL
  var SQS_QUEUE_URL = "";
  if (process && process.env && process.env.SqsQueueUrl) {
    SQS_QUEUE_URL = process.env.SqsQueueUrl;
  }

  // SQSの受信数指定は現時点で、これ以上大きくできない
  // 気軽に変更できないように環境変数化はしない
  var SQS_MAX_NUMBER_OF_MESSAGES = 10;

  // Lambdaの実行時間よりも大きくする（Lambdaの最大15分より少し大きく）
  // 気軽に変更できないように環境変数化はしない
  var SQS_VISIBILITY_TIMEOUT = 910;

  // 自身（Lambda）の再起呼び出し上限
  var SELF_RECALL_MAX = "3";
  if (process && process.env && process.env.SelfReCallMax) {
    SELF_RECALL_MAX = process.env.SelfReCallMax;
  }

  // 処理の実行
  function* executeBizUnitCommon(event, context, bizRequireObjects) {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# executeBizUnitCommon : start"
      );

      // 自動再実行ようの演算
      var reCallCount = event.reCallCount || 0;
      reCallCount += 1;
      event.reCallCount = reCallCount;

      // 読み込みモジュールの引き渡し
      AbstractSqsReceiveCommon.prototype.RequireObjects = bizRequireObjects;

      // 親の業務処理を実行
      return yield AbstractSqsReceiveCommon.prototype.executeBizCommon(
        event,
        context,
        bizRequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# executeBizUnitCommon : end"
      );
    }
  }
  AbstractSqsReceiveCommon.prototype.executeBizUnitCommon = executeBizUnitCommon;

  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.getExecutorsThreadsNum = function () {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getExecutorsThreadsNum : start"
      );
      return parseInt(EXECUTORS_THREADS_NUM);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getExecutorsThreadsNum : end"
      );
    }
  }.bind(AbstractSqsReceiveCommon); // AbstractSqsReceiveCommonをthisとする

  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.getExecutorsThreadsWait = function () {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getExecutorsThreadsWait : start"
      );
      return parseInt(EXECUTORS_THREADS_WAIT);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getExecutorsThreadsWait : end"
      );
    }
  }.bind(AbstractSqsReceiveCommon); // AbstractSqsReceiveCommonをthisとする

  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.getSqsReceiveMaxSize = function () {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getSqsReceiveMaxSize : start"
      );
      return parseInt(SQS_RECEIVE_MAX_SIZE);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getSqsReceiveMaxSize : end"
      );
    }
  }.bind(AbstractSqsReceiveCommon); // AbstractSqsReceiveCommonをthisとする

  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.getSqsReceiveErrRetryMax = function () {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getSqsReceiveErrRetryMax : start"
      );
      return parseInt(SQS_RECEIVE_ERR_RETRY_MAX);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getSqsReceiveErrRetryMax : end"
      );
    }
  }.bind(AbstractSqsReceiveCommon); // AbstractSqsReceiveCommonをthisとする

  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.getSelfReCallMax = function () {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("AbstractSqsReceiveCommon# getSelfReCallMax : start");
      return parseInt(SELF_RECALL_MAX);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractSqsReceiveCommon# getSelfReCallMax : end");
    }
  }.bind(AbstractSqsReceiveCommon); // AbstractSqsReceiveCommonをthisとする

  /*
  SQSからメッセージを受信する為のパラメータを返却
  */
  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.getSqsReceiveParams = function () {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getSqsReceiveParams : start"
      );

      var params = {
        QueueUrl: SQS_QUEUE_URL,
        MaxNumberOfMessages: SQS_MAX_NUMBER_OF_MESSAGES,
        VisibilityTimeout: SQS_VISIBILITY_TIMEOUT,
      };

      return params;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractSqsReceiveCommon# getSqsReceiveParams : end");
    }
  }.bind(AbstractSqsReceiveCommon); // AbstractSqsReceiveCommonをthisとする

  /*
  SQSからキュー情報（メッセージ残数）を取得するパラメータを返却する
  */
  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.getSqsQueueInfoParams = function () {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getSqsQueueInfoParams : start"
      );

      var params = {
        QueueUrl: SQS_QUEUE_URL,
        AttributeNames: [
          "ApproximateNumberOfMessages",
          "ApproximateNumberOfMessagesNotVisible",
        ],
      };

      return params;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getSqsQueueInfoParams : end"
      );
    }
  }.bind(AbstractSqsReceiveCommon); // AbstractSqsReceiveCommonをthisとする

  /*
  業務前処理

  SQSからメッセージを受信する

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.beforeMainExecute = function (
    args
  ) {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("AbstractSqsReceiveCommon# beforeMainExecute : start");

      var params = base.getSqsReceiveParams();
      base.writeLogDebug(
        "AbstractSqsReceiveCommon# SQS Param :" + JSON.stringify(params)
      );

      var result = {};
      var results = [];
      var retryCount = 0;
      var retryMax = base.getSqsReceiveErrRetryMax();
      var recordsMax = base.getSqsReceiveMaxSize();

      return new Promise(function (resolve, reject) {
        var sqsCallback = function (err, data) {
          if (err) {
            base.printStackTrace(err);
            retryCount++;

            if (retryCount <= retryMax) {
              base.writeLogInfo(
                "AbstractSqsReceiveCommon# Err Retry :" +
                  retryCount +
                  " Max:" +
                  retryMax
              );
              base.RequireObjects.Sqs.receiveMessage(params, sqsCallback);
            } else {
              reject(err);
            }
          } else {
            var datas = data.Messages || [];

            if (datas.length <= 0) {
              // キューに格納されたメッセージが0件になったので後続処理へ
              result.workerArgs = results;
              resolve(result);
            } else {
              // 前回の残りに今回、受信したメッセージを追加
              results = results.concat(datas);
              if (results.length >= recordsMax) {
                // 処理件数が蓄積されたので後続処理へ
                result.workerArgs = results;
                resolve(result);
              } else {
                // 処理件数が蓄積できるまで再起呼び出ししてSQSメッセージを取得する
                base.RequireObjects.Sqs.receiveMessage(params, sqsCallback);
              }
            }
          }
        };

        // SQS 受信処理　開始
        base.RequireObjects.Sqs.receiveMessage(params, sqsCallback);
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractSqsReceiveCommon# beforeMainExecute : end");
    }
  }.bind(AbstractSqsReceiveCommon.prototype.AbstractBaseCommon);

  /*
  業務メイン処理

  N件あるデータをワーカー処理（疑似スレッド処理）に引き渡し実行制御をする。

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.businessMainExecute = function (
    args
  ) {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# businessMainExecute : start"
      );

      // 直前のPromise処理の結果取得
      var beforePromiseResult = base.getLastIndexObject(args);

      //　ワーカー処理のクラス取得
      var subWorkerClazzFunc = base.getSubWorkerClazzFunc();

      var datas = beforePromiseResult.workerArgs || [];

      return new Promise(function (resolve, reject) {
        var executorsErrArray = [];
        var executorsResultArray = [];

        function sleep(msec, val) {
          return new Promise(function (resolve, reject) {
            setTimeout(resolve, msec, val);
          });
        }

        function* sub(record, threadsWait) {
          try {
            var workerFuncObj = new subWorkerClazzFunc();

            var result = yield workerFuncObj.execute(
              record,
              base.promiseRefs.context,
              base.RequireObjects
            );
            executorsResultArray.push(result);

            if (threadsWait > 0) {
              base.writeLogTrace(
                "AbstractSqsReceiveCommon# businessMainExecute : worker sleep:" +
                  String(threadsWait)
              );
              yield sleep(threadsWait, "Sleep Wait");
            }
          } catch (catchErr) {
            if (threadsWait > 0) {
              base.writeLogTrace(
                "AbstractSqsReceiveCommon# businessMainExecute : worker sleep:" +
                  String(threadsWait)
              );
              yield sleep(threadsWait, "Sleep Wait");
            }
            executorsErrArray.push(catchErr);
          }
        }

        function* controller(datas) {
          var results = [];
          try {
            var threadsNum = base.getExecutorsThreadsNum();
            var threadsWait = base.getExecutorsThreadsWait();
            var executors = base.RequireObjects.Executors(threadsNum);

            for (var i = 0; i < datas.length; i++) {
              var record = datas[i];
              results.push(executors(sub, record, threadsWait));
            }

            // Worker処理（疑似スレッド）の待ち合わせ
            yield results;

            // エラーハンドリング
            if (executorsErrArray.length > 0) {
              var irregularErr;
              for (var i = 0; i < executorsErrArray.length; i++) {
                var executorsErr = executorsErrArray[i];
                base.printStackTrace(executorsErr);

                if (!base.judgeBizError(executorsErr)) {
                  irregularErr = executorsErr;
                }
              }
              if (irregularErr) {
                throw irregularErr;
              }
            }

            return executorsResultArray;
          } catch (catchErr) {
            yield results;
            throw catchErr;
          }
        }

        base.RequireObjects.aa(controller(datas))
          .then(function (results) {
            resolve(results);
          })
          .catch(function (err) {
            reject(err);
          });
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractSqsReceiveCommon# businessMainExecute : end");
    }
  }.bind(AbstractSqsReceiveCommon.prototype.AbstractBaseCommon);

  /*
  業務後処理

  SQSからメッセージ残数を取得する

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.afterMainExecute = function (
    args
  ) {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("AbstractSqsReceiveCommon# afterMainExecute : start");

      var params = base.getSqsQueueInfoParams();
      base.writeLogDebug(
        "AbstractSqsReceiveCommon# SQS Param :" + JSON.stringify(params)
      );

      return new Promise(function (resolve, reject) {
        var sqsCallback = function (err, data) {
          if (err) {
            base.printStackTrace(err);
            reject(err);
          } else {
            resolve(data);
          }
        };

        // SQS キュー情報取得（残数）　開始
        base.RequireObjects.Sqs.getQueueAttributes(params, sqsCallback);
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractSqsReceiveCommon# afterMainExecute : end");
    }
  }.bind(AbstractSqsReceiveCommon.prototype.AbstractBaseCommon);

  /*
  業務後処理

  「SQSから取得したキュー情報」と「再実行回数」を条件に再実行するか判定する

  @param sqsResultInfos SQSから取得したキュー情報
  */
  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.reCallSelfCheck = function (
    sqsResultInfos
  ) {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("AbstractSqsReceiveCommon# reCallSelfCheck : start");

      var sendFlag = new Boolean(false);

      var reCallCount = base.promiseRefs.event.reCallCount;
      var reCallCountMax = base.getSelfReCallMax();

      var messageCount = 0;
      if (
        sqsResultInfos &&
        sqsResultInfos.Attributes &&
        sqsResultInfos.Attributes.ApproximateNumberOfMessages
      ) {
        messageCount = sqsResultInfos.Attributes.ApproximateNumberOfMessages;
      }

      // 上限に達していなくて、メッセージが残っている場合
      if (reCallCount <= reCallCountMax && messageCount > 0) {
        sendFlag = new Boolean(true);
      }

      base.writeLogInfo(
        "AbstractSqsReceiveCommon# retry # reCallCount:" +
          reCallCount +
          "# reCallCountMax:" +
          reCallCountMax +
          "# messageCount:" +
          messageCount
      );

      return sendFlag;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractSqsReceiveCommon# reCallSelfCheck : end");
    }
  }.bind(AbstractSqsReceiveCommon.prototype.AbstractBaseCommon);

  /*
  自身（Lambda）を再起呼び出しする為のパラメータを返却する
  */
  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.getLambdaExecuteFuncParam = function (
    event
  ) {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getLambdaExecuteFuncParam : start"
      );

      var funcName = process.env.AWS_LAMBDA_FUNCTION_NAME;

      var params = {
        FunctionName: funcName,
        InvokeArgs: JSON.stringify(event),
      };

      return params;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getLambdaExecuteFuncParam : end"
      );
    }
  }.bind(AbstractSqsReceiveCommon); // AbstractSqsReceiveCommonをthisとする

  /*
  業務後処理

  SQSからメッセージ残数を取得する

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.reCallSelfExecute = function (
    args
  ) {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("AbstractSqsReceiveCommon# reCallSelfExecute : start");

      // 前回の処理結果を取得
      var sqsResultInfos = base.getLastIndexObject(args);

      // 自身を呼び出すかチェックする
      var checkFlag = base.reCallSelfCheck(sqsResultInfos);

      if (checkFlag == true) {
        return new Promise(function (resolve, reject) {
          var params = base.getLambdaExecuteFuncParam(base.promiseRefs.event);
          base.writeLogDebug(
            "AbstractSqsReceiveCommon# Lambda Param :" + JSON.stringify(params)
          );

          base.RequireObjects.Lambda.invokeAsync(params, function (err, data) {
            if (err) {
              reject(err);
            } else {
              resolve(data);
            }
          });
        });
      } else {
        base.writeLogDebug("AbstractSqsReceiveCommon# Normal End");
        return "AbstractSqsReceiveCommon# Normal End";
      }
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractSqsReceiveCommon# reCallSelfExecute : end");
    }
  }.bind(AbstractSqsReceiveCommon.prototype.AbstractBaseCommon);

  /*
  ワーカー処理（疑似スレッド処理）のクラスを返却（オーバーライド必須）
  */
  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.getSubWorkerClazzFunc = function () {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getSubWorkerClazzFunc : start"
      );
      return require("./AbstractFirehosePutWorkerCommon.js");
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractSqsReceiveCommon# getSubWorkerClazzFunc : end"
      );
    }
  };

  /*
  順次処理する関数を指定する。
  Promiseを返却すると、Promiseの終了を待った上で順次処理をする
  
  前処理として、initEventParameterを追加

  @param event Lambdaの起動引数：event
  @param context Lambdaの起動引数：context
  */
  AbstractSqsReceiveCommon.prototype.AbstractBaseCommon.getTasks = function (
    event,
    context
  ) {
    var base = AbstractSqsReceiveCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("AbstractSqsReceiveCommon# getTasks :start");

      return [
        this.beforeMainExecute,
        this.businessMainExecute,
        this.afterMainExecute,
        this.reCallSelfExecute,
      ];
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractSqsReceiveCommon# getTasks :end");
    }
  };

  return {
    executeBizUnitCommon,
    AbstractSqsReceiveCommon,
    AbstractBizCommon: AbstractSqsReceiveCommon.prototype,
    AbstractBaseCommon: AbstractSqsReceiveCommon.prototype.AbstractBaseCommon,
  };
};
