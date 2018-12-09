/usr/local/src/spark-2.3.1-bin-hadoop2.7/bin/spark-submit\
        --class com.iris.streaming.HDFSWordCount\
        --master yarn-cluster \
        --executor-memory 1G \
        --total-executor-cores 2 \
        --files $HIVE_HOME/conf/hive-site.xml \
        ./spark-1.0-SNAPSHOT.jar \
        hdfs://master:9000/input/