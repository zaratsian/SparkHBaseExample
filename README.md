<h3>HBase Snapshot to Spark Example</h3>
<p>
This project shows how to analyze an HBase Snapshot using Spark. I used the the HBase <a href="https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableSnapshotInputFormat.html">TableSnapshotInputFormat</a> class, which allows allows a MapReduce job to run over a table snapshot.
<br>
<br>
Currently, there are a few different ways to process HBase Snapshots (using Hive, Spark, etc), but many of these methods cannot analyze the timestamps at the variable-level. This code shows how to filter/analyze the snapshot timestamps (and all the fields) at a granular level.
<br>
<br>Build project: <code>mvn package</code>
<br>
<br>Spark: <code>spark-submit --class com.github.zaratsian.SparkHBase.SparkHBase --master yarn-client /tmp/SparkHBaseExample-0.0.1-SNAPSHOT.jar props</code>
<br>
<br>
<br>
Here is the HBase table:
<img src="images/hbase_records.png" class="inline"/>
<br>
<br>
The output is a filtered list of records that are more recent (newer) than the user-defined timestamp
<img src="images/hbase_spark_output.png" class="inline"/>
<br>
This code was tested using <a href="http://hortonworks.com/products/data-center/hdp/">Hortonworks HDP</a> 2.4.2.0-258 
<br>HBase Version 1.1.2.2.4.2.0-258
<br>Spark Version 
</p>
