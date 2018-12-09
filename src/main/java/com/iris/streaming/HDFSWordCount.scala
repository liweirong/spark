package com.iris.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * /usr/local/src/spark-2.3.1-bin-hadoop2.7/examples/src/main/scala/org/apache/spark/examples/streaming/HdfsWordCount.scala
  * 执行 run_word_count 后即可完成任务的提交
  * kill任务
  * yarn application -kill application_1543936592361_0006
  */
object HDFSWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }


    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
   /* val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
*/
    val lines = ssc.socketTextStream("192.168.171.10",9999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    //保存到hdfs
    wordCounts.saveAsTextFiles(args(0))
    ssc.start()
    ssc.awaitTermination()

  }
}
