package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/*
Commands:
docker cp test.json spark-master:/opt/bitnami/spark/
docker cp target/SIC-Integration-1.0-SNAPSHOT.jar spark-master:/opt/bitnami/spark/
docker exec -it spark-master bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --class org.example.Main /opt/bitnami/spark/SIC-Integration-1.0-SNAPSHOT.jar
 */

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("StreamingTest").master("local[*]").getOrCreate();

        Dataset<Row> df = spark.read().option("multiline","true").json("/opt/bitnami/spark/test.json");
        df.show();
    }
}