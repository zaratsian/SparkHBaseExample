package com.github.zaratsian.SparkHBase;

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.HashMap
import scala.io.Source.fromFile




import com.typesafe.config.ConfigFactory  

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.util.Base64

import org.apache.hadoop.mapreduce.Job

import org.apache.hadoop.fs.Path
import java.lang.String

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._


object SparkHBase{

  def main(args: Array[String]) {
    val config = ConfigFactory.load()
    val hbaseRootDir =  "/apps/hbase"
    val sparkConf = new SparkConf()
      .setAppName("testnsnap")
      //.setMaster(config.getString("spark.app.master"))
      //.setJars(SparkContext.jarOfObject(this))
      //.set("spark.executor.memory", "2g")
      //.set("spark.default.parallelism", "160")

    val sc = new SparkContext(sparkConf)

    println("Creating hbase configuration")
    val conf = HBaseConfiguration.create()

    conf.set("hbase.rootdir", hbaseRootDir)
    conf.set("hbase.zookeeper.quorum",  "localhost:2181:/hbase-unsecure")
    val scan = new Scan
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val job = Job.getInstance(conf)

    TableSnapshotInputFormat.setInput(job, "customer_info_ss2", new Path("/tmp/.hbase-snapshot/customer_info_ss2"))

    val hBaseRDD = sc.newAPIHadoopRDD(job.getConfiguration, classOf[TableSnapshotInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    println(hBaseRDD.count())

    System.exit(0)
  }

  def convertScanToString(scan : Scan) = {
      val proto = ProtobufUtil.toScan(scan);
      Base64.encodeBytes(proto.toByteArray());
  }

}
