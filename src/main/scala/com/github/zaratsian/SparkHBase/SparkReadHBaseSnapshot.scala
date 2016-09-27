
/*******************************************************************************************************
This code does the following:
  1) Read an HBase Snapshot, and convert to Spark RDD (snapshot name is defined in props file)
  2) Parse the records / KeyValue (extracting column family, column name, timestamp, value, etc)
  3) Perform general data processing - Filter the data based on rowkey range AND timestamp (timestamp threshold variable defined in props file)
  4) Write the results to HDFS (formatted for HBase BulkLoad, saved as HFileOutputFormat)

Usage:

spark-submit --class com.github.zaratsian.SparkHBase.SparkReadHBaseSnapshot --jars /tmp/SparkHBaseExample-0.0.1-SNAPSHOT.jar /usr/hdp/current/phoenix-client/phoenix-client.jar /tmp/props

********************************************************************************************************/  

package com.github.zaratsian.SparkHBase;

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg

import scala.collection.mutable.HashMap
import scala.io.Source.fromFile
import scala.collection.JavaConverters._

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
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.text.SimpleDateFormat
import java.util.Arrays
import java.util.Date
import java.util.Calendar
import java.lang.String

object SparkReadHBaseSnapshot{
 
  case class hVar(rowkey: Int, colFamily: String, colQualifier: String, colDatetime: Long, colDatetimeStr: String, colType: String, colValue: String)
 
  def main(args: Array[String]) {

    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)
    
    val props = getProps(args(0))
    
    val sparkConf = new SparkConf().setAppName("SparkReadHBaseSnapshot")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    println("[ *** ] Creating HBase Configuration")
    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.rootdir", props.getOrElse("hbase.rootdir", "/tmp"))
    hConf.set("hbase.zookeeper.quorum",  props.getOrElse("hbase.zookeeper.quorum", "localhost:2181:/hbase-unsecure"))
    hConf.set(TableInputFormat.SCAN, convertScanToString(new Scan))

    val job = Job.getInstance(hConf)

    val path = new Path(props.getOrElse("hbase.snapshot.path", "/user/hbase"))
    val snapName = props.getOrElse("hbase.snapshot.name", "customer_info_ss")

    TableSnapshotInputFormat.setInput(job, snapName, path)

    val hBaseRDD = sc.newAPIHadoopRDD(job.getConfiguration, 
        classOf[TableSnapshotInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
    
    val record_count_raw = hBaseRDD.count() 
    println("[ *** ] Read in SnapShot (" + snapName.toString  + "), which contains " + record_count_raw + " records")
 
    // Extract the KeyValue element of the tuple
    val keyValue = hBaseRDD.map(x => x._2).map(_.list)

    //println("[ *** ] Printing raw SnapShot (10 records) from HBase SnapShot")         
    //hBaseRDD.map(x => x._1.toString).take(10).foreach(x => println(x))
    //hBaseRDD.map(x => x._2.toString).take(10).foreach(x => println(x))
    //keyValue.map(x => x.toString).take(10).foreach(x => println(x))

    val df = keyValue.flatMap(x =>  x.asScala.map(cell =>
        hVar(
            Bytes.toInt(CellUtil.cloneRow(cell)),
            Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
            Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
            cell.getTimestamp,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(cell.getTimestamp.toLong)),
            Type.codeToType(cell.getTypeByte).toString,
            Bytes.toStringBinary(CellUtil.cloneValue(cell))
            )
        )
    ).toDF()

    println("[ *** ] Printing parsed SnapShot (10 records) from HBase SnapShot")
    df.show(10, false)

    //Get timestamp (from props) that will be used for filtering
    val datetime_threshold      = props.getOrElse("datetime_threshold", "2016-08-25 14:27:02:001")
    val datetime_threshold_long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").parse(datetime_threshold).getTime()
    println("[ *** ] Filtering/Keeping all SnapShot records that are more recent (greater) than the datetime_threshold (set in the props file): " + datetime_threshold.toString)
    
    println("[ *** ] Filtering Dataframe")
    val df_filtered = df.filter($"colDatetime" >= datetime_threshold_long && $"rowkey".between(80001, 90000))

/*  // Filter RDD (alternative, but using a DF is a better option)
    val rdd_filtered = keyValue.flatMap(x => x.asScala.map(cell =>
          {(
            Bytes.toStringBinary(CellUtil.cloneRow(cell)),
            Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
            Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
            cell.getTimestamp,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(cell.getTimestamp.toLong)),
            Type.codeToType(cell.getTypeByte).toString,
            Bytes.toStringBinary(CellUtil.cloneValue(cell))
          )}
       )).filter(x => x._4>=datetime_threshold_long)
*/
 
    println("[ *** ] Filtered dataframe contains " + df_filtered.count() + " records")
    //println("[ *** ] Printing filtered HBase SnapShot records (10 records)")
    //df_filtered.show(10, false)

    // For testing purposes, print datatypes
    df_filtered.dtypes.toList.foreach(x => println(x))

    // Convert DF to KeyValue
    println("[ *** ] Converting dataframe to RDD so that it can be written as HFileOutputFormat using saveAsNewAPIHadoopFile")
    val rdd_from_df = df_filtered.rdd.map(x => {
        val kv: KeyValue = new KeyValue( Bytes.toBytes(x(0).asInstanceOf[Int]), x(1).toString.getBytes(), x(2).toString.getBytes(), x(3).asInstanceOf[Long], x(6).toString.getBytes() )
        (new ImmutableBytesWritable( Bytes.toBytes(x(0).asInstanceOf[Int]) ), kv)
    })

/*  // Convert RDD to KeyValue
    val rdd_to_hbase = rdd_filtered.map(x=>{
      val kv: KeyValue = new KeyValue(Bytes.toBytes(x._1), x._2.getBytes(), x._3.getBytes(), x._7.getBytes() )
        (new ImmutableBytesWritable(Bytes.toBytes(x._1)), kv)
      })
*/

    val time_snapshot_processing = Calendar.getInstance()
    println("[ *** ] Runtime for Snapshot Processing: " + ((time_snapshot_processing.getTimeInMillis() - start_time.getTimeInMillis()).toFloat/1000).toString + " seconds")

    // Configure HBase output settings
    val hTableName = snapName + "_filtered"
    val hConf2 = HBaseConfiguration.create()
    hConf2.set("zookeeper.znode.parent", "/hbase-unsecure")
    hConf2.set(TableOutputFormat.OUTPUT_TABLE, hTableName)

    //println("[ *** ] Saving results to HDFS as HBase KeyValue HFileOutputFormat. This makes it easy to BulkLoad into HBase (see SparkHBaseBulkLoad.scala for bulkload code)") 
    //rdd_from_df.map(x => x._2.toString).take(10).foreach(x => println(x))
    rdd_from_df.saveAsNewAPIHadoopFile("/tmp/" + hTableName, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], hConf2)



    // Print Total Runtime
    val end_time = Calendar.getInstance()
    println("[ *** ] End Time: " + end_time.getTime().toString)
    println("[ *** ] Saved " + rdd_from_df.count() + " records to HDFS, located in /tmp/" + hTableName.toString)
    println("[ *** ] Runtime for Snapshot Processing:                 " + ((time_snapshot_processing.getTimeInMillis() - start_time.getTimeInMillis()).toFloat/1000).toString + " seconds")
    println("[ *** ] Runtime for Snapshot Processing, saving to HDFS: " + ((end_time.getTimeInMillis() - start_time.getTimeInMillis()).toFloat/1000).toString + " seconds")   


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

//ZEND
