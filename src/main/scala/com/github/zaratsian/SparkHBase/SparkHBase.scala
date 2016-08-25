package com.github.zaratsian.SparkHBase;

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg
import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.mapred.JobConf

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat

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

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

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
    
    /* Print RDD record count */
    println(hBaseRDD.count())
    
    /* Print raw string of records in hBaseRDD snapshot 
    hBaseRDD.map(x => x._1.toString).take(10).foreach(x => println(x))
    */
    hBaseRDD.map(x => x._2.toString).take(10).foreach(x => println(x))

    val keyValue = hBaseRDD.map(x => x._2).map(_.list)

    val zout = keyValue.flatMap(x =>  x.asScala.map(cell =>
        "%s,%s,%s,%s,%s,%s".format(
          Bytes.toStringBinary(CellUtil.cloneRow(cell)),
          Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
          Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
          /*new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(cell.getTimestamp.toLong)),*/
          cell.getTimestamp,
          Type.codeToType(cell.getTypeByte),
          Bytes.toStringBinary(CellUtil.cloneValue(cell))
        )
      )
    )

    /* Output the parsed fields - showing the first 10 records */
    zout.take(10).foreach(x => println(x))

    /* Set the date threshold (all records that are more recent (newer) than this date will be output */
    val date_string = props.getOrElse("filter_date", "2016-08-12 11:46:00:001")

    val dt_num = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").parse(date_string).getTime()

    val zout_filtered = zout.map(x => x.split(',')).filter(x => {var temp = x(3).toLong; temp>dt_num})

    /*
    zout.map(x => x.split(',')).map(x => {var temp = x(2).replace("timestamp=","").toLong; new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(temp))}).collect().foreach(x => println(x))
    */

    println("[ *** ] Printing filtered records as DataFrame")

    zout_filtered.map(cell =>
        {(
          cell(0).toString,
          cell(1).toString,
          cell(2).toString,
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(cell(3).toLong)),
          cell(4).toString,
          cell(5).toString
        )}
    ).toDF().show(20, false)

    // Save RDD to HDFS
    println("[ *** ] Saving filtered RDD to HDFS")
    zout_filtered.saveAsTextFile("/tmp/hbase_data_from_spark")

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


  def convertToPut(cell: Array[String]): (ImmutableBytesWritable, Put) = {
    var out_rowid  = cell(0).toString;
    var out_family = cell(1).toString;
    var out_column = cell(2).toString;
    var out_timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(cell(3).toLong));
    var out_value = cell(5).toString;
    
    var puttohbase = new Put(new String(out_rowid).getBytes());
        puttohbase.add("tableinfo".getBytes(), out_column.getBytes(), new String(out_value).getBytes())
    
    return (new ImmutableBytesWritable(Bytes.toBytes(out_rowid)), puttohbase)
  }

}
