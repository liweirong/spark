package com.iris.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectOffsetDem {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val Array(brokers, topics, consumer) = Array("192.168.171.10:9092", "test", "group_test")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectOffsetDem")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "group.id" -> consumer)


    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder](ssc, kafkaParams, topicsSet)
    var offsetRanges = Array[OffsetRange]() // 相当于Bloker

    messages.foreachRDD { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // offSize:最新的位置
      for (offSize <- offsetRanges) {
        km.commitOffsetsToZK(offsetRanges)
        print(s"${offSize.topic} ${offSize.partition} ${offSize.fromOffset}  ${offSize.untilOffset}")
        //        badou 0 2798598  2798627
      }
    }

    messages.map(_._2).map((_, 1L)).reduceByKey(_ + _).print

    ssc.start()
    ssc.awaitTermination()
  }
}
