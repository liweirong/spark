package com.iris.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 8 sparkStreaming
  *
  *  在192.168.171.10设备上输入 ]# nc -lp 9999  输入后回车即可与此连接
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[2]")
    val sc = new StreamingContext(conf,Seconds(2))
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val lines = sc.socketTextStream("192.168.171.10",9999)
    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map((_,1)).reduceByKey(_+_)
//    wordCounts.print()
//    //1 这种方法每两秒都会输出，没有输入就没有输出


//    val wordCounts2 = words.map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(20),Seconds(2)) // 必须是倍数
//    wordCounts2.print()
// 2 窗口滑动  有些数据在里面存放20秒


    sc.checkpoint("D:\\workspace\\idea_workspace\\git_project\\sparkStreamingTmp")
    val addFunc=(curValues:Seq[Long],preValueState:Option[Long])=>{
      val curCount = curValues.sum // 当前两秒的批次求和
      val preCount = preValueState.getOrElse(0L)
      Some(curCount+ preCount)
    }
    val wordCounts3 = words.map((_,1L)).updateStateByKey[Long](addFunc)
    wordCounts3.print()

    //必须设置checkPoint，不然报错 ：java.lang.IllegalArgumentException: requirement failed: The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint().

    sc.start()
    sc.awaitTermination()





  }
}
