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

    val props = getProps(args(0))

    val sparkConf = new SparkConf().setAppName(props.getOrElse("spark.appName", "testsnap"))
    val sc = new SparkContext(sparkConf)

    println("Creating hbase configuration")
    val conf = HBaseConfiguration.create()

    conf.set("hbase.rootdir", props.getOrElse("hbase.rootdir", "/apps/hbase/data"))
    conf.set("hbase.zookeeper.quorum",  props.getOrElse("hbase.zookeeper.quorum", "localhost:2181:/hbase-unsecure"))

    val scan = new Scan 
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val job = Job.getInstance(conf)

    val path = new Path(props.getOrElse("hbase.snapshot.path", "/user/hbase"))
    val snapName = props.getOrElse("hbase.snapshot.name", "customer_info_ss")
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
          /*new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(cell.getTimestamp.toLong)),*/
          cell.getTimestamp,
          Type.codeToType(cell.getTypeByte),
          Bytes.toStringBinary(CellUtil.cloneValue(cell))
        )
      )
    )



    /* Output the first 10 records */
    zout.take(10).foreach(x => println(x))



    /* Set the date threshold (all records that are more recent (newer) than this date will be output */
    val date_string = "2016-08-12 11:46:26:800"

    val dt_num = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").parse(date_string).getTime()

    val zout_filtered = zout.map(x => x.split(',')).filter(x => {var temp = x(2).replace("timestamp=","").toLong; temp>dt_num})

    /*
    zout.map(x => x.split(',')).map(x => {var temp = x(2).replace("timestamp=","").toLong; new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(temp))}).collect().foreach(x => println(x))
    */

    zout_filtered.map(cell =>
        "%s,%s,%s,%s,%s".format(
          cell(0).toString,
          cell(1).toString,
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(cell(2).replace("timestamp=","").toLong)),
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
