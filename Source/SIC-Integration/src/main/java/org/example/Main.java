package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import WorkPackage.SmartInsectCounting.Bronze.DataInput;

import static org.apache.spark.sql.types.DataTypes.createDecimalType;


/*
Commands:
docker cp test.json spark-master:/opt/bitnami/spark/inputTest
docker cp target/SIC-Integration-1.0-SNAPSHOT.jar spark-master:/opt/bitnami/spark/
docker exec -it spark-master bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0 --master spark://spark-master:7077 --class org.example.Main /opt/bitnami/spark/SIC-Integration-1.0-SNAPSHOT.jar
 */

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("StreamingTest").master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.1.0")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        DataInput inputStream = new DataInput(spark);
        try {
            inputStream.startStream();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Dataset<Row> jsonData = spark.read().option("multiline","true").json("/opt/bitnami/spark/test.json");

        Dataset<Row> df = jsonData
                .select(jsonData.col("imageID").as("CardID"), functions.explode(jsonData.col("findings")).as("finding"))
                .select("CardID", "finding.id", "finding.probability").withColumnRenamed("id","InsectID");

        df.write().mode("overwrite").parquet("/opt/bitnami/spark/test/");

        df.write().format("delta").mode("overwrite").save("/opt/bitnami/spark/tmp/delta-table");

        Dataset<Row> df2 = spark.read().format("delta").load("/opt/bitnami/spark/tmp/delta-table");

        Dataset<Row> findings = df2
                .groupBy("InsectID")
                .agg(
                        functions.count("InsectID").as("InsectCount"),
                        functions.avg("probability").as("accuracy")
                );

        findings.show();
        findings.write().format("delta").mode("overwrite").save("/opt/bitnami/spark/tmp/InsectFindings");
    }
}