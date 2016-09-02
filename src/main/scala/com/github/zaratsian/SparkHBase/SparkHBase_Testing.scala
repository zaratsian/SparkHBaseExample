
package com.github.zaratsian.SparkHBase;

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles


object SparkHBaseLoad{

  def main(args: Array[String]) {
    
    println("[ *** ] Initializing Spark App: spark_hbase_loader")
    val conf = new SparkConf().setAppName("spark_hbase_loader").setMaster("yarn-client")
    val sc = new SparkContext(conf)
    
    val tableName = "zhbasetable"
    
    val hconf = HBaseConfiguration.create()
    hconf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hconf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    
/*
    val table = new HTable(hconf, tableName) 
    
    val job = Job.getInstance(hconf)
    job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass (classOf[KeyValue])
    
    HFileOutputFormat.configureIncrementalLoad (job, table)
*/   
    
    // Generate 10 sample data:
    val rdd = sc.parallelize(1 to 10)

    println("[ *** ] Printing first 5 records of Spark RDD containing the HBase KeyValue")
    rdd.take(5).foreach(x => println((x, (x, "cf","c1","value_xxx"))))

    val rdd_out = rdd.map(x=>{
        val kv: KeyValue = new KeyValue(Bytes.toBytes(x), "cf".getBytes(), "c1".getBytes(), "value_xxx".getBytes() )
        (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    })
    
    println("[ *** ] Saving HFiles to HDFS") 
    rdd_out.saveAsNewAPIHadoopFile("/tmp/spark_hbasetable", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], hconf)
    
    //Bulk load Hfiles to Hbase
/*
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path("/tmp/xxxx19"), table)
*/



    sc.stop()

  }

}
