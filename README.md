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
Here is the sample HBase:
<br>
hbase(main):006:0* scan "customer_info"
ROW                                           COLUMN+CELL                                                                   
 1                                            column=profile:balance, timestamp=1471027586646, value=100000                  
 1                                            column=profile:location, timestamp=1471027586577, value=north carolina        
 1                                            column=profile:name, timestamp=1471027586494, value=frank                      
 2                                            column=profile:balance, timestamp=1471027586812, value=90000                   
 2                                            column=profile:location, timestamp=1471027586742, value=new york               
 2                                            column=profile:name, timestamp=1471027586707, value=dean                       
 3                                            column=profile:balance, timestamp=1471027588418, value=75000                   
 3                                            column=profile:location, timestamp=1471027586882, value=nevada                 
 3                                            column=profile:name, timestamp=1471027586854, value=sammy                      
3 row(s) in 0.8790 seconds
<br>
<br>
This code was tested using <a href="http://hortonworks.com/products/data-center/hdp/">Hortonworks HDP</a> 2.4.2.0-258 
<br>HBase Version 1.1.2.2.4.2.0-258
<br>Spark Version 
</p>
