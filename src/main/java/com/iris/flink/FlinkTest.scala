package com.iris.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 需要加入下面这个import才能在idea中跑本地flink：
  * import org.apache.flink.streaming.api.scala._
  */
object FlinkTest {
  def main(args: Array[String]): Unit = {
    //        val benv = ExecutionEnvironment.getExecutionEnvironment
    //        val data = benv.readTextFile("")
    //        data.map((_,1L)).groupBy(0).sum(1).print()

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val data = senv.socketTextStream("192.168.171.10", 9999, '\n')

    val wordCounts = data
      /*
          //1.简单wordCount
          data.flatMap { w => w.split("\\s") }.map(w => WordCount(w, 1)).keyBy("word")
          .sum("count") //  - >WordCount(cv,5)
      */

      /*    //2.Window操作
          data.flatMap { w => w.split("\\s") }.map(w => WordCount(w, 1)).keyBy("word")
          .timeWindow(Time.seconds(5), Time.seconds(1)) // 持续五秒
           // .countWindowAll(5) // WordCount(w,5)  满足5条才打印第一个  r c c c c  =>输出 （r，5） - 五次告警时间取第一次
          // .countWindow(10,2) // 10：最多装10个WordCount(w,10) ；2：步长
          .sum("count")*/

      // 3.max操作 输入 a,10
      .map(w => WordCount(w.split(",")(0), w.split(",")(1).toLong))
      .keyBy("word")
      .max("count") // 根据输入的（key,value）  key取value的最大值


    wordCounts.print().setParallelism(1)
    senv.execute("Stream")
  }

  case class WordCount(word: String, count: Long)

}
