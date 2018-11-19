package com.iris.offline

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      System.err.println("Usage: SparkWordCount <inputfile> <outputfile>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("WominWordCount")
    val sc = new SparkContext(conf)
    val textRdd = sc.textFile(args(0))
    val result = textRdd.flatMap ( line => line.split("\\s+") ).map(word => (word, 1)).reduceByKey(_ + _)
    result.saveAsTextFile(args(1))
  }
}
