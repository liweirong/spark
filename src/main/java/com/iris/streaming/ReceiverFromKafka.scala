package com.iris.streaming


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.functions._

object ReceiverFromKafka {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }

    val Array(group_id, topic, exectime, dt) = args
    val zkHostIP = Array("10", "11", "12").map("192.168.171." + _)
    val ZK_QUORUM = zkHostIP.map(_ + ":2181").mkString(",")
    //192.168.174.134:2181,192.168.174.125:2181,192.168.174.129:2181

    val numThreads = 1

    // 创建streamContext
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))
    // topic 参数 对应线程Map
    val topicSet = topic.split(",").toSet
    val topicMap = topicSet.map((_, numThreads.toInt)).toMap

    // 通过Receiver接受kafka数据 获取message value
    // 为什么是map（_._2）: @return DStream of (Kafka message key, Kafka message value)
    val mesR = KafkaUtils.createStream(ssc, ZK_QUORUM, group_id, topicMap).map(_._2)

    def rdd2DF(rdd: RDD[String]): DataFrame = {
      val spark = SparkSession
        .builder()
        .appName("Streaming Frome Kafka")
        .config("hive.exec.dynamic.partition", "true") // 动态分区
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport().getOrCreate()

      // 涉及隐式转换
      import spark.implicits._
      // python去写 - > Json格式的数据 ??  {"order_id":"1","user_id":"2"}   ??
      case class Order(order_id: String, user_id: String)

      rdd.map { x =>
        val mess = JSON.parseObject(x, classOf[Orders])
        Order(mess.order_id, mess.user_id)
      }.toDF()

    }

    val log = mesR.foreachRDD { rdd =>
      val df = rdd2DF(rdd)
      df.withColumn("dt", lit(dt.toString))
        .write.mode(SaveMode.Append)
        .insertInto("bigData.order_partition") //数据追加到这个表中
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
