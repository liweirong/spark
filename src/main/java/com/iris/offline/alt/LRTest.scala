package com.iris.offline.alt

import com.iris.offline.feat.SimpleFeature
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LRTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("LR")
      .enableHiveSupport()
      .getOrCreate()

    val orders = spark.sql("select * from bigdata.orders")
    val priors = spark.sql("select * from bigdata.order_products_prior")
    val trains = spark.sql("select * from bigdata.trains")

    val (prodFeat,userFeat) = SimpleFeature.Feat(priors,orders)
//    prior,train,test用户集合的情况分析 结论：train+test=prior
    //    131209
    val train_user = orders.filter("eval_set='train'").select("user_id").distinct()
//    75000
    val test_user = orders.filter("eval_set='test'").select("user_id").distinct()
//    206209
    val prior_user = orders.filter("eval_set='prior'").select("user_id").distinct()
//    0
    val interset_user = train_user.intersect(test_user)
//    131209
    val train_inter = prior_user.intersect(train_user)
//    75000
    val test_inter = prior_user.intersect(test_user)

    val op = orders.join(priors,"order_id") //eval_set= prior
    val optrain = orders.join(trains,"order_id") //eval_set = train

//  prior去重样本数量：13307953
    val user_recall = op.select("user_id","product_id").distinct()
//    user_real与user_recall的交集是828823，user_real数据集：1384616
    val user_real = optrain.select("user_id","product_id").distinct()
      .withColumn("label",lit(1))
//    正样本：1384616 ，负样本：12479130
    val trainData = user_recall.join(user_real,Seq("user_id","product_id"),"outer").na.fill(0)

    val train = trainData.join(userFeat,"user_id").join(prodFeat,"product_id")

//  模型

// 具体特征：Array(product_id, user_id, label, u_avg_day_gap, user_ord_cnt, u_prod_dist_cnt,
// u_prod_records, u_avg_ord_prods, prod_sum_rod, prod_rod_rate, prod_cnt)
//    特征处理通过rformula:离散化特征one-hot，连续特征不处理，
//    最后将分别处理的特征向量拼成最后的特征
    val rformula = new RFormula()
  .setFormula("label ~ u_avg_day_gap +user_ord_cnt + u_prod_dist_cnt" +
  "+u_avg_ord_prods+prod_sum_rod+prod_rod_rate+prod_cnt")
  .setFeaturesCol("features")
  .setLabelCol("label")

//    对数据进行rformula处理生成新的数据：features是特征向量，label是标签
    val df = rformula.fit(train).transform(train).select("features","label")
//      .cache()
//  算法为收敛，迭代停止是因为达到了做大迭代次数
// LogisticRegression training finished but the result is not converged because: max iterations reached
//    lr模型的定义
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0)

//    划分训练集和测试集
    val Array(trainingData,testData)=df.randomSplit(Array(0.7,0.3))
//    模型训练
    val lrModel = lr.fit(trainingData)
    // 打印系数（weight：W）和截距b
    print(s"Coefficients: ${lrModel.coefficients} intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary
    val objectHistory = trainingSummary.objectiveHistory
//   打印loss
    objectHistory.foreach(loss=>println(loss))

    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    val roc = binarySummary.roc
//   TPR,FPR
    roc.show()
//    AUC
    println(binarySummary.areaUnderROC)
//    预测testData
    val test = lrModel.transform(testData)

  }

}
