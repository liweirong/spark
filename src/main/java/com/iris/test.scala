package com.iris

import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val testRdd = spark.sparkContext.textFile("D:\\workspace\\idea_workspace\\git_project\\spark\\com\\iris\\The_Man_of_Property.txt")
    testRdd.flatMap(_.split(" ").map((_,1))).reduceByKey(_+_)
      .sortBy(_._2,ascending = false).take(20)
      .foreach(println)
  }

}
