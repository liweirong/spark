package com.iris.streaming.streaming_hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 方式一：直接使用HBase Table的PUT方法
  */
object SparkPut {
  /**
    * insert 100,000 cost 20762 ms
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkPut")
    val context = new SparkContext(conf)

    try {
      val rdd = context.makeRDD(1 to 100000, 4)

      // column family
      val family = Bytes.toBytes("cf")
      // column counter --> ctr
      val column = Bytes.toBytes("ctr")

      println("count is :" + rdd.count())
      rdd.take(5).foreach(println)

      // mapPartition & foreachPartition
      // mapPartition is a lazy transformation, if no action, there is no result.
      // foreachPartition is an action
      rdd.foreachPartition(list => {
        val table = createTable()
        list.foreach(value => {
          val put = new Put(Bytes.toBytes(value))
          put.addImmutable(family, column, Bytes.toBytes(value))
          table.put(put)
        })
        table.close()
      })
    } finally {
      context.stop()
    }
  }

  /**
    * create Hbase Table interface.
    *
    * @return
    */
  def createTable(): Table = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "localhost")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.defaults.for.version.skip", "true")
    val conn = ConnectionFactory.createConnection(hbaseConf)
    conn.getTable(TableName.valueOf("test_table"))
  }

}
