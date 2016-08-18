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

import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.KeyValue.Type


import java.text.SimpleDateFormat
import java.util.Arrays
import java.util.Date



object SparkHBase{

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("testsnap")
    val sc = new SparkContext(sparkConf)

    println("Creating hbase configuration")
    val conf = HBaseConfiguration.create()

    conf.set("hbase.rootdir", "/apps/hbase/data")
    conf.set("hbase.zookeeper.quorum", "localhost:2181:/hbase-unsecure")

    val scan = new Scan 
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val job = Job.getInstance(conf)

    val path = new Path("/user/hbase")
    val snapName = "customer_info_ss"
    TableSnapshotInputFormat.setInput(job, snapName, path)

    val hBaseRDD = sc.newAPIHadoopRDD(job.getConfiguration, classOf[TableSnapshotInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    println(hBaseRDD.count())

    
    hBaseRDD.foreach(e=> ( println("%s".format( Bytes.toString(e._1.get()) ) ) ))
    

    val keyValue = hBaseRDD.map(x => x._2).map(_.list)

    val zout = keyValue.flatMap(x =>  x.asScala.map(cell =>
        "columnFamily=%s,qualifier=%s,timestamp=%s,type=%s,value=%s".format(
          Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
          Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
          cell.getTimestamp,
          Type.codeToType(cell.getTypeByte),
          Bytes.toStringBinary(CellUtil.cloneValue(cell))
        )
      )
    )


    zout.collect().foreach(x => println(x))



   
val date_string = "2016-08-12 12:59:59:800"

val dt_num = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS").parse(date_string).getTime()

val results = zout.map(x => x.split(',')).filter(x => {var temp = x(2).replace("timestamp=","").toLong; temp>dt_num})

results.map(cell =>
        "%s,%s,%s,%s,%s".format(
          cell(0).toString,
          cell(1).toString,
          cell(2).toString,
          cell(3).toString,
          cell(4).toString
        )
    ).collect().foreach(x => println(x))

 

    sc.stop()

    
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
