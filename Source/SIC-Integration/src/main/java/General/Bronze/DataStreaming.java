package General.Bronze;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataStreaming {

    /*
    General Structured streaming function for streaming files from pulsar
    Should stay active once run, or until spark application termination

    spark - the spark session object
    serviceUrl - link to pulsar service url ex:"pulsar://localhost:6650"
    adminUrl - link to admin url ex:"http://localhost:8080"
    topic - identifier of the message stream within pulsar ex:"persistent://tenant/namespace/topic-name"
    outputPath - path to output of parquet files converted from stream
    checkpointPath - path to checkpoint location (needed in case of errors)
    */
    public static void dataStream (SparkSession spark, String serviceUrl, String adminUrl, String topic, String outputPath, String checkpointPath) throws Exception{
        Dataset<Row> fileStream = spark.readStream()
                .format("pulsar")
                .option("service.url", serviceUrl)
                .option("admin.url", adminUrl)
                .option("topic", topic)
                .load();

        fileStream.writeStream()
                .format("parquet")
                .option("path", outputPath)
                .option("checkpointLocation", checkpointPath)
                .start()
                .awaitTermination();
    }

    public static void testStream(SparkSession spark, String outputPath, String checkpointLocation) throws  Exception {

        StructType findingsSchema = new StructType(new StructField[]{
                DataTypes.createStructField("x", DataTypes.DoubleType, false),
                DataTypes.createStructField("y", DataTypes.DoubleType, false),
                DataTypes.createStructField("width", DataTypes.DoubleType, false),
                DataTypes.createStructField("height", DataTypes.DoubleType, false),
                DataTypes.createStructField("stretched", DataTypes.BooleanType, false),
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("slug", DataTypes.StringType, false),
                DataTypes.createStructField("colorProfile", DataTypes.StringType, false),
                DataTypes.createStructField("probability", DataTypes.DoubleType, false)
        });

        // Define the schema for the main JSON structure
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("imageId", DataTypes.StringType, false),
                DataTypes.createStructField("version", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("colorProfile", DataTypes.StringType, false),
                DataTypes.createStructField("findings", DataTypes.createArrayType(findingsSchema), false)
        });

        Dataset<Row> fileStream = spark.readStream()
                .schema(schema)
                .json("/opt/bitnami/spark/inputTest");

        fileStream.writeStream()
                .format("parquet")
                .option("path",outputPath)
                .option("checkpointLocation", checkpointLocation)
                .start()
                .awaitTermination();
    }
}
