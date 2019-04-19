package com.iris.streaming

import com.iris.util.ConnectionPool
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectOffsetDem {

  case class WordCount(word: String, count: Int)

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

    val result1 = messages.map(_._2).map((_, 1L)).reduceByKey(_ + _)
    result1.print()

    /**
      * 存mysql
      */
    result1.foreachRDD(rdd => {
      // 错误示范1：在driver创建连接，在woker使用。会报错connection object not serializable。
      // val conn = ConnectionPool.getConnection();
      rdd.foreachPartition(eachPartition => {
        // 错误示范2：rdd每个记录都创建连接，成本非常高。
        // 正确示范：拿到rdd以后foreachPartition，每个partition创建连接，而且使用数据库连接池。
        val conn = ConnectionPool.getConnection()
        eachPartition.foreach(word => {
          val sql = "insert into WordCount(word,count) values('" + word._1 + "'," + word._2 + ")"
          val stmt = conn.createStatement
          stmt.executeUpdate(sql)
        })
        ConnectionPool.returnConnection(conn)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
