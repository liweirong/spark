package com.iris.cf

import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.SparkSession

object UserBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("User Base CF").enableHiveSupport()
      .getOrCreate()
    val udata = spark.sql("select * from bigdata.udata")

    import spark.implicits._
    // 1.计算相似用户  cosine = a*b/(|a|*|b|)
    // 所有分母 |a|= sqrt(a1^2+a2^2+...+an^2) 平方求和开根号
    val userScoreSum = udata.rdd.map(x => (x(0).toString, x(2).toString))
      .groupByKey()
      .mapValues(x => sqrt(x.toArray.map(rating => pow(rating.toDouble, 2)).sum))
      // userScoreSum.take(10)
      // -> res2: Array[(String, Double)] = Array((273,17.378147196982766), (528,28.160255680657446), (584,17.916472867168917), (736,16.186414056238647), (456,52.40229002629561), (312,66.83561924602779), (62,53.0659966456864), (540,29.866369046136157), (627,46.62617290749907), (317,17.08800749063506))
      .toDF("user_id", "rating_sqrt_sum") // 用户 对应的分母

    // 1.1 item -> user 倒排表

    val df = udata.selectExpr("user_id as user_v", "item_id as item_id", "rating as rating_v") //|user_v|item_id|rating_v|
    val df_decare = udata.join(df, "item_id")
    //    |item_id|user_id|rating|timestamp|user_v|rating_v|
    //    +-------+-------+------+---------+------+--------+
    //    |    242|    196|     3|881250949|   721|       3|
    //    |    242|    196|     3|881250949|   720|       4|
    //    |    242|    196|     3|881250949|   500|       3|
    //dot
    import org.apache.spark.sql.functions._
    val product_udf = udf((s1: Int, s2: Int) => s1.toDouble * s2.toDouble)
    val df_product = df_decare.withColumn("rating_product", product_udf(col("rating"), col("rating_v")))
      .select("user_id", "user_v", "rating_product")

    // 求和，计算完整的分子部分
    val df_sim_group = df_product.groupBy("user_id", "user_v")
      .agg("rating_product" -> "sum")
      .withColumnRenamed("sum(rating_product)", "rating_dot")

    val userScoreSum_v = userScoreSum.selectExpr("user_id as user_v",
      "rating_sqrt_sum as rating_sqrt_sum_v")
    val df_sim = df_sim_group.join(userScoreSum, "user_id")
      .join(userScoreSum_v, "user_v")
      .selectExpr("user_id", "user_v",
        "rating_dot / (rating_sqrt_sum * rating_sqrt_sum_v) as cosine_sim")

    //    |user_v|rating_v|cosine_sim|
    //    +------+--------+----------+
    //    |   125|     296| 0.271716124
    /** 至此对应的用户相似度已经计算好了 */


  }
}
