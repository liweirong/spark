package com.iris.streaming

import org.apache.log4j.{Level, Logger}
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
//        if (args.length < 1) {
//          System.err.println("Usage: HdfsWordCount <directory>")
//          System.exit(1)
//        }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val sparkConf = new SparkConf()
      .setAppName("HdfsWordCount").setMaster("local[2]") //将逻辑扩展到集群上运行，分配给Spark Streaming应用程序的核心数量必须大于接收者的数量。否则，系统将只接收数据，但不会处理它。
    val ssc = new StreamingContext(sparkConf, Seconds(2))
//    ssc.checkpoint("/code/spark/temp")
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    /* val lines = ssc.textFileStream(args(0))
     val words = lines.flatMap(_.split(" "))
 */
    val lines = ssc.socketTextStream("master", 9999)
    val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//    val addFunc = (curValues: Seq[Long], preValues: Option[Long]) => {
//      val curValue = curValues.sum
//      val preValue = preValues.getOrElse(0L) // 历史信息需要缓存，如果需要记录今天的话需要用时间的判断来清理历史数据
//      Some(curValue + preValue)
//    }
    // 一整天的销量pv uv
//    val wordCounts = lines.map((_, 1L)).updateStateByKey[Long](addFunc)
    wordCounts.print()
    //保存到hdfs
//    wordCounts.saveAsTextFiles(args(0))
    ssc.start()
    ssc.awaitTermination()

  }
}
