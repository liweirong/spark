package com.iris.streaming.streaming_hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions

/**
  * Description: Use Mutator batch insert in spark context.
  *
  */
object SparkPutList {

  /**
    * Use mutator batch insert 100,000, mutator.mutator(Put) cost: 22369
    * Use put list insert 100,000, cost: 25571
    * Use put list by Map 100,000, cost: 21299
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //    putByList()
    putByMap()
  }

  def putByMap(): Unit = {
    val conf = new SparkConf().setAppName(SparkPutList.getClass().getSimpleName())
    val context = new SparkContext(conf)

    // column family
    val family = Bytes.toBytes("cf")
    // column counter --> ctr
    val column = Bytes.toBytes("ctr")

    try {
      val rdd = context.makeRDD(1 to 100000, 4)
      rdd.map(value => {
        val put = new Put(Bytes.toBytes(value))
        put.addImmutable(family, column, Bytes.toBytes(value))
      }).foreachPartition(
        itr => {
          val hbaseConf = HBaseConfiguration.create()
          val conn = ConnectionFactory.createConnection(hbaseConf)
          val table = conn.getTable(TableName.valueOf("test_table"))
          table.put(JavaConversions.seqAsJavaList(itr.toSeq))
          table.close()
        })
    } finally {
      context.stop()
    }
  }

  def putByList(): Unit = {
    val conf = new SparkConf().setAppName(SparkPutList.getClass().getSimpleName())
    val context = new SparkContext(conf)

    // column family
    val family = Bytes.toBytes("cf")
    // column counter --> ctr
    val column = Bytes.toBytes("ctr")

    try {
      val rdd = context.makeRDD(1 to 100000, 4)
      rdd.foreachPartition(list => {
        val hbaseConf = HBaseConfiguration.create()
        val conn = ConnectionFactory.createConnection(hbaseConf)
        val table = conn.getTable(TableName.valueOf("test_table"))
        val putList = new java.util.LinkedList[Put]()
        list.foreach(value => {
          val put = new Put(Bytes.toBytes(value))
          put.addImmutable(family, column, Bytes.toBytes(value))
          putList.add(put)
        })
        table.put(putList)
        table.close()
      })
    } finally {
      context.stop()
    }
  }

  def putByMutator(): Unit = {
    val conf = new SparkConf().setAppName(SparkPutList.getClass().getSimpleName())
    val context = new SparkContext(conf)

    // column family
    val family = Bytes.toBytes("cf")
    // column counter --> ctr
    val column = Bytes.toBytes("ctr")

    try {
      val rdd = context.makeRDD(1 to 100000, 4)
      rdd.foreachPartition(list => {
        val hbaseConf = HBaseConfiguration.create()
        val conn = ConnectionFactory.createConnection(hbaseConf)
        val mutator = conn.getBufferedMutator(TableName.valueOf("test_table"))
        list.foreach(value => {
          val put = new Put(Bytes.toBytes(value))
          put.addImmutable(family, column, Bytes.toBytes(value))
          mutator.mutate(put)
        })
        mutator.close()
      })
    } finally {
      context.stop()
    }
  }
}

 
