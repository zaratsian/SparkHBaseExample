
/*******************************************************************************************************
This code does the following:
  1) Creates HBase table (if it does not already exist) 
  2) Uses LoadIncrementalHFiles to BulkLoad from HDFS to HBase Table 
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

object SparkHBaseBulkLoad{
 
  def main(args: Array[String]) {

    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)
    
    val props = getProps(args(0))
    
    val sparkConf = new SparkConf().setAppName("SparkHBaseBulkLoad")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
 
    // HBase table name (if it does not exist, it will be created) 
    val hTableName   = "sparkhbasebulkload"
    val columnFamily = "demographics" 
    
    println("[ *** ] Creating HBase Configuration")
    val hConf = HBaseConfiguration.create()
    hConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hConf.set(TableInputFormat.INPUT_TABLE, hTableName)

    //val job = new Job (hConf, "DumpHFile")
    //job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    //job.setMapOutputValueClass (classOf[KeyValue])
    
    val table = new HTable(hConf, hTableName)

    // Create HBase Table
    val admin = new HBaseAdmin(hConf)
    if(!admin.isTableAvailable(hTableName)) {
        println("[ ***] Creating HBase Table ( " + hTableName + " )")
        val hTableDesc = new HTableDescriptor(hTableName)
        hTableDesc.addFamily(new HColumnDescriptor(columnFamily.getBytes()))
        admin.createTable(hTableDesc)
    }else{
        print("[ *** ] HBase Table ( " + hTableName + " ) already exists!!")
        val columnDesc = new HColumnDescriptor(columnFamily.getBytes())
	admin.disableTable(Bytes.toBytes(hTableName))
        admin.addColumn(hTableName, columnDesc)
	admin.enableTable(Bytes.toBytes(hTableName))
    }

/*  Uncomment this for testing purposes
    // Generating Test Data
    println("[ *** ] Generating Test Data as an RDD")
    val rdd = sc.parallelize(1 to 10)

    println("[ *** ] Printing first 5 records of Spark RDD containing the HBase KeyValue structure")
    rdd.take(5).foreach(x => println((x, (x, "cf","c1","value_xxx"))))

    val rdd_out = rdd.map(x => {
        val kv: KeyValue = new KeyValue(Bytes.toBytes(x), columnFamily.getBytes(), "c1".getBytes(), "value_xxx".getBytes() )
        (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    })
*/


    println("[ *** ] BulkLoading from HDFS (HFileOutputFormat) to HBase Table (" + hTableName  + ")")
    val bulkLoader = new LoadIncrementalHFiles(hConf)
    bulkLoader.doBulkLoad(new Path("/tmp/" + hTableName), table)


    sc.stop()


    // Print Runtime Metric
    val end_time = Calendar.getInstance()
    println("[ *** ] End Time: " + end_time.getTime().toString)
    println("[ *** ] Total Runtime: " + ((end_time.getTimeInMillis() - start_time.getTimeInMillis()).toFloat/1000).toString + " seconds")   

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
