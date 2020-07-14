/*
処理固有に必要な処理などを、この層に実装する。

テストや挙動確認を含めたコードをコメントアウト込みで、
サンプルとして記述する。

written by syo
http://awsblog.physalisgp02.com
*/
module.exports = function SampleSqsReceiveModule() {
  // 疑似的な継承関係として親モジュールを読み込む
  var superClazzFunc = new require("./AbstractSqsReceiveCommon.js");
  // prototypeにセットする事で継承関係のように挙動させる
  SampleSqsReceiveModule.prototype = new superClazzFunc();

  // 処理の実行
  function* execute(event, context, bizRequireObjects) {
    var base = SampleSqsReceiveModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("SampleSqsReceiveModule# execute : start");

      // 親の業務処理を実行
      return yield SampleSqsReceiveModule.prototype.executeBizUnitCommon(
        event,
        context,
        bizRequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("SampleSqsReceiveModule# execute : end");
    }
  }

  /*
  ワーカー処理（疑似スレッド処理）用のクラスを返却
  */
  SampleSqsReceiveModule.prototype.AbstractBaseCommon.getSubWorkerClazzFunc = function () {
    var base = SampleSqsReceiveModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "SampleSqsReceiveModule# getSubWorkerClazzFunc : start"
      );
      return require("./SampleFirehosePutWorkerModule.js");
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("SampleSqsReceiveModule# getSubWorkerClazzFunc : end");
    }
  };

  return {
    execute,
  };
};
