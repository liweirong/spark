/usr/local/src/spark-2.0.2-bin-hadoop2.6/bin/spark-submit\
        --class com.iris.hbase.SparkHbase\
	    --master yarn-cluster \
        --executor-memory 1G \
        --total-executor-cores 2 \
        --files $HIVE_HOME/conf/hive-site.xml \
        --jars $HIVE_HOME/lib/mysql-connector-java-5.1.44-bin.jar,$SPARK_HOME/jars/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/jars/datanucleus-core-3.2.10.jar,$SPARK_HOME/jars/datanucleus-rdbms-3.2.9.jar,$SPARK_HOME/jars/guava-14.0.1.jar\
	./spark-1.0-SNAPSHOT-jar-with-dependencies.jar
