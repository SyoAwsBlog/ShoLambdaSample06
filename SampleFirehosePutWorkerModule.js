module.exports = function SampleFirehosePutWorkerModule() {
  // 疑似的な継承関係として親モジュールを読み込む
  var superClazzFunc = require("./AbstractFirehosePutWorkerCommon.js");
  // prototypeにセットする事で継承関係のように挙動させる
  SampleFirehosePutWorkerModule.prototype = new superClazzFunc();

  function* execute(event, context, RequireObjects) {
    var base = SampleFirehosePutWorkerModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("SampleFirehosePutWorkerModule# execute : start");

      return yield SampleFirehosePutWorkerModule.prototype.executeBizWorkerCommon(
        event,
        context,
        RequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("SampleFirehosePutWorkerModule# execute : end");
    }
  }
  SampleFirehosePutWorkerModule.prototype.execute = execute;

  /*
  Firehoseへの出力カラムをDynamoDB項目名の配列で返却する

  */
  SampleFirehosePutWorkerModule.prototype.AbstractBaseCommon.getFileOutputColumns = function (
    record
  ) {
    var base = SampleFirehosePutWorkerModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "SampleFirehosePutWorkerModule# getFileOutputColumns : start"
      );
      var cols = ["PrimaryKey", "SortKey"];
      return cols;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "SampleFirehosePutWorkerModule# getFileOutputColumns : end"
      );
    }
  }.bind(SampleFirehosePutWorkerModule.prototype.AbstractBaseCommon);

  // 関数定義は　return　より上部に記述
  // 外部から実行できる関数をreturnすること
  return {
    execute,
    SampleFirehosePutWorkerModule,
    AbstractBaseCommon:
      SampleFirehosePutWorkerModule.prototype.AbstractBaseCommon,
    AbstractBizCommon:
      SampleFirehosePutWorkerModule.prototype.AbstractBizCommon,
    AbstractWorkerChildCommon: SampleFirehosePutWorkerModule.prototype,
  };
};
