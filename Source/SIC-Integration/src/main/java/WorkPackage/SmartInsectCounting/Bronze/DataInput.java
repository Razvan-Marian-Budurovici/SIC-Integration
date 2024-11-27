package WorkPackage.SmartInsectCounting.Bronze;

import org.apache.spark.sql.SparkSession;

import static General.Bronze.DataStreaming.dataStream;
import static General.Bronze.DataStreaming.testStream;

public class DataInput {

    String serviceURL = "",
            adminURL = "",
            topic = "",
            outputPath = "/opt/bitnami/spark/WP_3/Bronze/Data",
            checkpointPath = "/opt/bitnami/spark/WP_3/Bronze/DataCheckpoint";
    SparkSession spark;

    public DataInput(SparkSession spark){
        this.spark = spark;
    }

    public void startStream() throws Exception{
        testStream(spark, outputPath, checkpointPath);
        //dataStream(spark,serviceURL,adminURL,topic,outputPath,checkpointPath);
    }
}
