package com.iris.streaming


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

/**
  * 此类运行打包时要传递依赖包，因为kafkaUtils、json需要依赖
  */
object ReceiverFromKafka {

  // python去写 - > Json格式的数据 ??  {"order_id":"1","user_id":"2"}   ??
  case class Order(order_id: String, user_id: String)

  def main(args: Array[String]): Unit = {
    //    if (args.length < 4) {
    //      System.err.println("Usage: HdfsWordCount <directory>")
    //      System.exit(1)
    //
    //    }

    // 0.对日志进行控制
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    //1.获取参数
    //    val Array(group_id, topic, exectime, dt) = args
    val Array(group_id, topic, exectime, dt) = Array("group_test", "test", "20", "20190405") //exectime:批次时间
    val zkHostIP = Array("10", "11", "12").map("192.168.171." + _)
    val ZK_QUORUM = zkHostIP.map(_ + ":2181").mkString(",")
    //192.168.174.134:2181,192.168.174.125:2181,192.168.174.129:2181

    //线程数
    val numThreads = 1

    // 2.创建streamContext
    val conf = new SparkConf() //.setAppName("test receiver").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))
    // topic 参数 对应线程Map
    val topicSet = topic.split(",").toSet
    val topicMap = topicSet.map((_, numThreads.toInt)).toMap

    // 3.通过Receiver接受kafka数据 获取message value
    // 为什么是map（_._2）: @return DStream of (Kafka message key, Kafka message value) 第一个是key，一般没有是null
    val mesR = KafkaUtils.createStream(ssc, ZK_QUORUM, group_id, topicMap).map(_._2)

    mesR.map((_, 1L)).reduceByKey(_ + _).print

    // 4.生成一个rdd转df的方法，供后面使用
    def rdd2DF(rdd: RDD[String]): DataFrame = {
      val spark = SparkSession
        .builder()
        .appName("Streaming From Kafka")
        .config("hive.exec.dynamic.partition", "true") // 动态分区，每天一个分区
        .config("hive.exec.dynamic.partition.mode", "nonstrict") //
        .enableHiveSupport().getOrCreate()

      // 涉及隐式转换
      import spark.implicits._


      // 解析数据
      rdd.map { x =>
        val mess = JSON.parseObject(x, classOf[Orders])
        Order(mess.order_id, mess.user_id) //结构在上方定义
      }.toDF()

    }

    // 5.DF核心处理逻辑，对每个rdd做“rdd转df”，然后通过df结构将数据追加到分区表中
    val log = mesR.foreachRDD { rdd =>
      val df = rdd2DF(rdd)
      df.withColumn("dt", lit(dt.toString)) // 增加一列，时间戳
        .write.mode(SaveMode.Append)
        .insertInto("bigData.order_partition") //数据追加到这个表中
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
