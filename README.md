<h3>Hbase to Spark Example</h3>
<p>
<br>This is a work in progress. The goal is to analyze an Hbase Snapshot with Spark (at the variable-level with timestamps).
<br>
<br>Build project: <code>mvn package</code>
<br>
<br>Spark: <code>spark-submit --class com.github.randerzander.SparkHBase.SparkHBase --master yarn-client /tmp/SparkHBaseExample-0.0.1-SNAPSHOT.jar --executor-memory 512M --num-executors 1</code>
</p>
