package com.iris.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

object SparkHbase {

// 从hive取数据Dataframe->RDD写入hbase
  def main(args: Array[String]): Unit = {
    //    HBase zookeeper
    val ZOOKEEPER_QUORUM = "192.168.171.10,192.168.171.11,192.168.171.12"
    val spark = SparkSession.builder().appName("spark to Hbase")
      .enableHiveSupport().getOrCreate()

    val rdd = spark.sql("select order_id,user_id,order_dow from bigdata.orders limit 300")
      .rdd

    /**
      * 一个put对象就是一行记录，在构造方法中指定主键user_id
      * 所有插入的数据必须用org.apache.hadoop.hbase.util.toBytes方法转换
      **/
    rdd.map { row =>
      val order_id = row(0).asInstanceOf[String]
      val user_id = row(1).asInstanceOf[String]
      val order_dow = row(2).asInstanceOf[String]
//     加处理逻辑

      var p = new Put(Bytes.toBytes(user_id))
      p.add(Bytes.toBytes("id"), Bytes.toBytes("order"), Bytes.toBytes(order_id))
      p.add(Bytes.toBytes("num"), Bytes.toBytes("dow"), Bytes.toBytes(order_dow))
      p
    }.foreachPartition {partition=>
      //初始化jobconf,TableOutputFormat必须在org.apache.hadoop.hbase.mapred包下
      val jobConf = new JobConf(HBaseConfiguration.create())
      jobConf.set("hbase.zookeeper.quorum",ZOOKEEPER_QUORUM)
      jobConf.set("hbase.zookeeper.property.clientPort","2181")
      jobConf.set("zookeeper.znode.parent","/hbase")
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      //写入的表名
      val table = new HTable(jobConf,TableName.valueOf("orders"))
//      把
      import scala.collection.JavaConversions._
      table.put(seqAsJavaList(partition.toSeq))
    }

  }

}
