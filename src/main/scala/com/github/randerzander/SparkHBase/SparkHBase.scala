package com.github.randerzander.SparkHBase;

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

object SparkHBase{
  def main(args: Array[String]) {
    val props = getProps(args(0))

    val srcTable = getArrayProp(props, "srcTables").map(t=>t.toUpperCase)

    val zkUrl = props.getOrElse("zkUrl", "localhost:2181:/hbase-unsecure")

    // Create SparkContext
    val sparkConf = new SparkConf().setAppName("SparkHBase")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.newAPIHadoopRDD...

    sc.stop()
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
