package com.iris.offline

import org.apache.hadoop.hive.ql.optimizer.spark.SparkSkewJoinProcFactory.SparkSkewJoinJoinProcessor
import org.apache.spark.sql.SparkSession

object FuncTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Simple Test")
      .master("local[2]")
      .getOrCreate()
    val df = spark.sql("select * from badou.orders")
    import spark.implicits._
    val orderNumberSort = df.select("user_id","order_number","order_hour_of_day")
      .rdd.map(x=>(x(0).toString,(x(1).toString,x(2).toString)))
      .groupByKey()
      .mapValues{
        _.toArray.sortWith(_._2<_._2)
      }.toDF("user_id","ons")

    // udf
    import org.apache.spark.sql.functions._
    val plusUDF = udf((col1:String,col2:String)=>col1.toInt+col2.toInt)
    df.withColumn("plus_udf",plusUDF(col("order_number"),col("order_dow"))).show()




  }

}
