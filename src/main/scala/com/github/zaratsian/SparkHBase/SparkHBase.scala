
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

object SparkHBase{
 
  case class hVar(rowkey: String, colFamily: String, colQualifier: String, colDatetime: Long, colDatetimeStr: String, colType: String, colValue: String)
 
  def main(args: Array[String]) {

    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)
    
    val props = getProps(args(0))
    
    val sparkConf = new SparkConf().setAppName(props.getOrElse("spark.appName", "testsnap"))
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    println("[ *** ] Creating HBase Configuration")
    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.rootdir", props.getOrElse("hbase.rootdir", "/apps/hbase/data"))
    hConf.set("hbase.zookeeper.quorum",  props.getOrElse("hbase.zookeeper.quorum", "localhost:2181:/hbase-unsecure"))

    val scan = new Scan 
    hConf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val job = Job.getInstance(hConf)

    val path = new Path(props.getOrElse("hbase.snapshot.path", "/user/hbase"))
    val snapName = props.getOrElse("hbase.snapshot.name", "customer_info_ss")
    TableSnapshotInputFormat.setInput(job, snapName, path)

    val hBaseRDD = sc.newAPIHadoopRDD(job.getConfiguration, classOf[TableSnapshotInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
   
    val record_count_raw = hBaseRDD.count() 
    println("[ *** ] Read in SnapShot (" + snapName.toString  + "), which contains " + record_count_raw + " records")
 
    // Extract the KeyValue element of the tuple
    val keyValue = hBaseRDD.map(x => x._2).map(_.list)

    println("[ *** ] Printing raw SnapShot (10 records) from HBase SnapShot")         
    //hBaseRDD.map(x => x._1.toString).take(10).foreach(x => println(x))
    //hBaseRDD.map(x => x._2.toString).take(10).foreach(x => println(x))
    keyValue.map(x => x.toString).take(10).foreach(x => println(x))

    val zout = keyValue.flatMap(x =>  x.asScala.map(cell =>
        hVar(
            Bytes.toStringBinary(CellUtil.cloneRow(cell)),
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
    zout.show(10, false)
    //zout.take(10).foreach(x => println(x))

    val datetime_threshold      = props.getOrElse("datetime_threshold", "2016-08-12 11:46:00:001")
    val datetime_threshold_long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").parse(datetime_threshold).getTime()
    println("[ *** ] Filtering/Keeping all SnapShot records that are more recent (greater) than the datetime_threshold (set in the props file): " + datetime_threshold.toString)
    
    val zout_filtered  = zout.filter($"colDatetime" >= datetime_threshold_long)
    val zout_filtered2 = keyValue.flatMap(x => x.asScala.map(cell =>
          {(
            Bytes.toStringBinary(CellUtil.cloneRow(cell)),
            Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
            Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
            cell.getTimestamp,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(cell.getTimestamp.toLong)),
            Type.codeToType(cell.getTypeByte).toString,
            Bytes.toStringBinary(CellUtil.cloneValue(cell))
          )}
       ))

    println("[ *** ] Data has been filtered down to " + zout_filtered.count() + " records out of " + record_count_raw)
    println("[ *** ] Printing filtered HBase SnapShot records (10 records)")
    zout_filtered.show(10, false)

    /*
    zout_filtered2
    (496832,demographics,age,1472159181696,2016-08-25 14:06:21:696,Put,100)
    (496832,demographics,custid,1472159181696,2016-08-25 14:06:21:696,Put,3025161)
    (496832,demographics,gender,1472159181696,2016-08-25 14:06:21:696,Put,male)
    (496832,demographics,level,1472159181696,2016-08-25 14:06:21:696,Put,gold)
    (4968320,demographics,age,1472160474783,2016-08-25 14:27:54:783,Put,67)
    */
    
    val rdd_to_hbase = zout_filtered2.map(x=>{
      val kv: KeyValue = new KeyValue(Bytes.toBytes(x._1), x._2.getBytes(), x._3.getBytes(), x._7.getBytes() )
        (new ImmutableBytesWritable(Bytes.toBytes(x._1)), kv)
      })


/* 
    //Create Hbase Table

    val tableName = "zcreate_hbasetable"

    val conf = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    //conf.set(TableInputFormat.INPUT_TABLE, tableName)

    //val job2 = Job.getInstance(conf)
    val job2 = new Job (conf, "DumpHFile")

    job2.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    job2.setMapOutputValueClass (classOf[KeyValue])
    val table = new HTable(conf, tableName)
    HFileOutputFormat.configureIncrementalLoad (job2, table)

    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(tableName)) {
      println("[ ***] Creating HBase Table")
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor("demographics".getBytes()));
      admin.createTable(tableDesc)
    }else{
      print("[ *** ] Table already exists!!")
      val columnDesc = new HColumnDescriptor("demographics");
	  admin.disableTable(Bytes.toBytes(tableName));
      admin.addColumn(tableName, columnDesc);
	  admin.enableTable(Bytes.toBytes(tableName));
    }
*/


    val tableName = "zhbasetable"

    val conf = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    println("[ *** ] Saving HFiles / Results to HDFS")
    rdd_to_hbase.saveAsNewAPIHadoopFile("/tmp/hbase_from_spark", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], conf)

    //Bulk load Hfiles to Hbase
    //val bulkLoader = new LoadIncrementalHFiles(conf)
    //bulkLoader.doBulkLoad(new Path("/tmp/xxxx19"), table)


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
