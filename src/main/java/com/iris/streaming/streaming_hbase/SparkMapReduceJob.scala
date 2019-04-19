package com.iris.streaming.streaming_hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: Put data into Hbase by map reduce Job.
  */
class SparkMapReduceJob {


  /**
    * insert 100,000 cost 21035 ms
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkPutByMap")
    val context = new SparkContext(conf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "test_table")
    //IMPORTANT: must set the attribute to solve the problem (can't create path from null string )
    hbaseConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")

    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    try {
      val rdd = context.makeRDD(1 to 100000)

      // column family
      val family = Bytes.toBytes("cf")
      // column counter --> ctr
      val column = Bytes.toBytes("ctr")

      rdd.map(value => {
        var put = new Put(Bytes.toBytes(value))
        put.addImmutable(family, column, Bytes.toBytes(value))
        (new ImmutableBytesWritable(), put)
      })
        .saveAsNewAPIHadoopDataset(job.getConfiguration)
    } finally {
      context.stop()
    }
  }

}
