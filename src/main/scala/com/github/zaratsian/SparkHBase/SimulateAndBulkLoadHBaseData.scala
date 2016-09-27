
/*******************************************************************************************************

This code:
  1) Simulates X number of HBase records (number of simulated records can be defined in props file)
  2) Bulkloads the data into HBase (table name can be defined in props file)

Usage:

spark-submit --class com.github.zaratsian.SparkHBase.SimulateAndBulkLoadHBaseData --jars /tmp/SparkHBaseExample-0.0.1-SNAPSHOT.jar /usr/hdp/current/phoenix-client/phoenix-client.jar /tmp/props  
 
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
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer

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
import util.Random

object SimulateAndBulkLoadHBaseData{
 
  def main(args: Array[String]) {

    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)
    
    val props = getProps(args(0))
    val number_of_simulated_records = props.getOrElse("simulated_records", "1000").toInt 

    val sparkConf = new SparkConf().setAppName("SimulatedHBaseTable")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
 
    // HBase table name (if it does not exist, it will be created) 
    val hTableName   = props.getOrElse("simulated_tablename", "hbase_simulated_table")
    val columnFamily = "cf"
    
    println("[ *** ] Creating HBase Configuration")
    val hConf = HBaseConfiguration.create()
    hConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hConf.set(TableInputFormat.INPUT_TABLE, hTableName)

    val table = new HTable(hConf, hTableName)

    // Create HBase Table
    val admin = new HBaseAdmin(hConf)
    
    if(!admin.isTableAvailable(hTableName)) {

        println("[ *** ] Simulating Data")
        val rdd = sc.parallelize(1 to number_of_simulated_records)

        // Setup Random Generator
        //val rand = scala.util.Random

        println("[ *** ] Creating KeyValues")
        val rdd_out = rdd.map(x => {            
            val kv: KeyValue = new KeyValue( Bytes.toBytes(x), columnFamily.getBytes(), "c1".getBytes(), x.toString.getBytes() ) 
            (new ImmutableBytesWritable( Bytes.toBytes(x) ), kv)
        })

        println("[ *** ] Printing simulated data (10 records)")
        rdd_out.map(x => x._2.toString).take(10).foreach(x => println(x))

        println("[ ***] Creating HBase Table (" + hTableName + ")")
        val hTableDesc = new HTableDescriptor(hTableName)
        hTableDesc.addFamily(new HColumnDescriptor(columnFamily.getBytes()))
        admin.createTable(hTableDesc)

        println("[ *** ] Saving data to HDFS as KeyValue/HFileOutputFormat (table name = " + hTableName + ")")
        val hConf2 = HBaseConfiguration.create()
        hConf2.set("zookeeper.znode.parent", "/hbase-unsecure")
        hConf2.set(TableOutputFormat.OUTPUT_TABLE, hTableName)
        rdd_out.saveAsNewAPIHadoopFile("/tmp/" + hTableName, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], hConf2)

        println("[ *** ] BulkLoading from HDFS (HFileOutputFormat) to HBase Table (" + hTableName  + ")")
        val bulkLoader = new LoadIncrementalHFiles(hConf2)
        bulkLoader.doBulkLoad(new Path("/tmp/" + hTableName), table)

    }else{
        println("[ *** ] HBase Table ( " + hTableName + " ) already exists!")
        println("[ *** ] Stopping the simulation. Remove the existing HBase table and try again.")
    }


    sc.stop()


    // Print Runtime Metric
    val end_time = Calendar.getInstance()
    println("[ *** ] Created a table (" + hTableName + ") with " + number_of_simulated_records.toString + " records within HDFS and also bulkloaded it into HBase")
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
