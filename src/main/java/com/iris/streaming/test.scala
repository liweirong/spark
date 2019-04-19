package com.iris.streaming

import com.iris.util.ConnectionPool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 8 sparkStreaming
  *
  * 在192.168.171.10设备上输入 ]# nc -lp 9999  输入后回车即可与此连接
  */
object test {
  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext with two working thread and batch interval of 2 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val conf = new SparkConf().setAppName("WC").setMaster("local[2]")
    val sc = new StreamingContext(conf, Seconds(2))
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = sc.socketTextStream("192.168.171.10", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    //    val wordCounts = words.map((_,1)).reduceByKey(_+_)
    //    wordCounts.print()
    //    //1 这种方法每两秒都会输出，没有输入就没有输出


    //    val wordCounts2 = words.map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(20),Seconds(2)) // 必须是sc设置的倍数 - 2的倍数
    //    wordCounts2.print()
    // 2 窗口滑动  有些数据在里面存放20秒


    sc.checkpoint("D:\\workspace\\idea_workspace\\git_project\\sparkStreamingTmp")
    /**
      * a:20
      * b:30
      */
    val addFunc = (curValues: Seq[Long], preValueState: Option[Long]) => {
      val curCount = curValues.sum // 当前两秒的批次求和
      val preCount = preValueState.getOrElse(0L) // 以前批次的数据
      Some(curCount + preCount)
    }
    val wordCounts3 = words.map((_, 1L)).updateStateByKey[Long](addFunc)
    wordCounts3.print()

    //updateStateByKey 全局统计量 必须设置checkPoint，不然报错 ：java.lang.IllegalArgumentException: requirement failed: The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint().


    /**
      * 结果存mysql - -成功
      */
    wordCounts3.foreachRDD(rdd => {
      // 错误示范1：在driver创建连接，在woker使用。会报错connection object not serializable。
      // val conn = ConnectionPool.getConnection();
      rdd.foreachPartition(eachPartition => {
        // 错误示范2：rdd每个记录都创建连接，成本非常高。
        // 正确示范：拿到rdd以后foreachPartition，每个partition创建连接，而且使用数据库连接池。
        val conn = ConnectionPool.getConnection()
        eachPartition.foreach(word => {
          val sql = "insert into word_count (word,count,time) values('" + word._1 + "'," + word._2 + ",1)"
          val stmt = conn.createStatement
          stmt.executeUpdate(sql)
        })
        ConnectionPool.returnConnection(conn)
      })
    })

    sc.start() // Start the computation
    sc.awaitTermination() // Wait for the computation to terminate


  }
}
