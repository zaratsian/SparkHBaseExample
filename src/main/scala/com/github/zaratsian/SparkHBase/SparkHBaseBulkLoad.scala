
/******************************************************************************************
This code:
  1) Reads an HBase Snapshot into a Spark Dataframe
  2) Parses the records
  3) Processes/Filters the data (additional analytics can be done on the records as well)
  4) Writes the results (as DataFrame) to HDFS
******************************************************************************************/  

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

object SparkHBaseBulkLoad{
 
  def main(args: Array[String]) {

    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)
    
    val props = getProps(args(0))
    
    val sparkConf = new SparkConf().setAppName(props.getOrElse("spark.appName", "sparkhbasebulkload"))
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    println("[ *** ] Creating HBase Configuration")
    val hConf = HBaseConfiguration.create()
    //hConf.set("hbase.rootdir", props.getOrElse("hbase.rootdir", "/apps/hbase/data"))
    //hConf.set("hbase.zookeeper.quorum",  props.getOrElse("hbase.zookeeper.quorum", "localhost:2181:/hbase-unsecure"))

    // Setup HBase Configuation
    val tableName = "sparkhbasebulkload"

    hConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    //hConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hConf.set(TableInputFormat.INPUT_TABLE, tableName)

    //val job = Job.getInstance(conf)
    val job = new Job (hConf, "DumpHFile")

    job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass (classOf[KeyValue])
    val table = new HTable(hConf, tableName)
    //HFileOutputFormat.configureIncrementalLoad (job, table)

    // Create HBase Table
    val admin = new HBaseAdmin(hConf)
    if(!admin.isTableAvailable(tableName)) {
      println("[ ***] Creating HBase Table")
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor("cf".getBytes()));
      admin.createTable(tableDesc)
    }else{
      print("[ *** ] Table already exists!!")
      val columnDesc = new HColumnDescriptor("cf");
	  admin.disableTable(Bytes.toBytes(tableName));
      admin.addColumn(tableName, columnDesc);
	  admin.enableTable(Bytes.toBytes(tableName));
    }





    // Generate 10 sample data:
    val rdd = sc.parallelize(1 to 10)

    println("[ *** ] Printing first 5 records of Spark RDD containing the HBase KeyValue")
    rdd.take(5).foreach(x => println((x, (x, "cf","c1","value_xxx"))))

    val rdd_out = rdd.map(x=>{
        val kv: KeyValue = new KeyValue(Bytes.toBytes(x), "cf".getBytes(), "c1".getBytes(), "value_xxx".getBytes() )
        (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    })
    
    println("[ *** ] Saving HFiles to HDFS") 
    rdd_out.saveAsNewAPIHadoopFile("/tmp/sparkhbasebulkload", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], hConf)
    








    //Bulk load Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(hConf)
    bulkLoader.doBulkLoad(new Path("/tmp/sparkhbasebulkload"), table)


    sc.stop()


    // Print Runtime Metric
    val end_time = Calendar.getInstance()
    println("[ *** ] End Time: " + end_time.getTime().toString)
    println("[ *** ] Total Runtime: " + ((end_time.getTimeInMillis() - start_time.getTimeInMillis()).toFloat/1000).toString + " seconds")   

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

//ZEND
