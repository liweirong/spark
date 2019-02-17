package com.iris.bayes

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, StringIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BayesTest {
  def main(args: Array[String]): Unit = {

    val ModelPath = "hdfs:///mid_data/nb/bayes_model"
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxSize","800")
//    建立spark session，传入conf
    val spark = SparkSession
      .builder()
      .appName("Bayes Test")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

//    定义结巴分词的方法，传入DF，输出DF，多一列seg，分好的词
    def JiebaSeg(df:DataFrame,colname:String): DataFrame ={
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)

      val jieba_udf = udf{(sentence:String)=>
        val segV = seg.value
        segV.process(sentence.toString,SegMode.INDEX)
          .toArray()
          .map(_.asInstanceOf[SegToken].word)
//          .filter(_.length>1)
      }
      df.withColumn("seg",jieba_udf(col(colname)))
    }

//    从hive里取新闻数据（new_no_seg）
    val df = spark.sql("select * from bigdata.news_noseg")
    df.show()
//    调用结巴对新闻进行切词
    val df_seg = JiebaSeg(df,"sentence").select("seg","label")
    df_seg.show()
//    val df = spark.sql("select regexp_replace(seg,'/',' ') as seg,label from badou.news_jieba")

//    word 进行hashing编码和TF统计
    val TF = new HashingTF()
      .setBinary(false)
      .setInputCol("seg")
      .setOutputCol("rawFeatures")

    val df_tf = TF.transform(df_seg).select("rawFeatures","label")
    df_tf.show()
    //对word进行idf加权
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .setMinDocFreq(1)

    val idfModel = idf.fit(df_tf) //
    val df_tfidf = idfModel.transform(df_tf).select("features","label")
    df_tfidf.show()

//    类别是英文的，如auto，yule. 对string类型的label做Double类型的编码
    val stringIndex = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexed")
      .setHandleInvalid("error")

    val df_tfidf_lab = stringIndex.fit(df_tfidf).transform(df_tfidf)
    df_tfidf_lab.show()
//    对训练集和测试集进行80%，20%进行划分
    val Array(train,test) = df_tfidf_lab.randomSplit(Array(0.8,0.2))

//    对bayes模型进行实例化定义参数
    val nb = new NaiveBayes()
      .setModelType("multinomial")
      .setSmoothing(1.0)
      .setFeaturesCol("features")
      .setLabelCol("indexed")
      .setPredictionCol("pred_label")
      .setProbabilityCol("prob")
      .setRawPredictionCol("rawPred")

//    模型训练
    val nbModel = nb.fit(train)

//    模型预测
    val pred = nbModel.transform(test)

//    模型评估 F1值，
    /**
      * param for metric name in evaluation (supports `"f1"` (default), `"weightedPrecision"`,
      * `"weightedRecall"`, `"accuracy"`)
      */
    val eval = new MulticlassClassificationEvaluator()
      .setLabelCol("indexed")
      .setPredictionCol("pred_label")
      .setMetricName("f1")

    val f1_score = eval.evaluate(pred)
    println("Test f1 score = "+f1_score)

//    hdfs path
    nbModel.save(ModelPath)
    println(s"Bayes Model have saved in $ModelPath")
  }

}
