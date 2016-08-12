package com.github.zaratsian.SparkHBase;

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

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
    val props = getProps(args(0))

    val sparkConf = new SparkConf().setAppName(props.getOrElse("spark.appName", "test"))
    val sc = new SparkContext(sparkConf)

    println("Creating hbase configuration")
    val conf = HBaseConfiguration.create()

    conf.set("hbase.rootdir", props.getOrElse("hbase.rootdir", "/apps/hbase/data"))
    conf.set("hbase.zookeeper.quorum",  props.getOrElse("hbase.zookeeper.quorum", "localhost:2181:/hbase-unsecure"))

    val scan = new Scan
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val job = Job.getInstance(conf)

    val path = new Path(props.getOrElse("hbase.snapshot.path", "/apps/hbase/data/.hbase-snapshot"))
    val snapName = props.getOrElse("hbase.snapshot.name", "test")
    TableSnapshotInputFormat.setInput(job, snapName, path)

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

  def getArrayProp(props: => HashMap[String,String], prop: => String): Array[String] = {
    return props.getOrElse(prop, "").split(",").filter(x => !x.equals(""))
  }

  def getProps(file: => String): HashMap[String,String] = {
    var props = new HashMap[String,String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }
}
