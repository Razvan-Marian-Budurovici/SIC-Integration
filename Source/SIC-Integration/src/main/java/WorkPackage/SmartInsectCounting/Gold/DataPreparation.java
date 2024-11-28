package WorkPackage.SmartInsectCounting.Gold;


import General.Bronze.DataSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static General.Silver.DataOrganization.fileStream;

public class DataPreparation {

    SparkSession spark;
    String insectSource = "/opt/bitnami/spark/WP_3/Silver/Data/insectFindingTable",
        cardSource = "/opt/bitnami/spark/WP_3/Silver/Data/yellowCardTable",
        insectPopulationTablePath = "/opt/bitnami/spark/WP_3/Gold/Data/insectPopulation",
        insectPopulationTablePathCheckpoint = "/opt/bitnami/spark/WP_3/Gold/DataCheckpoint/insectPopulation";

    public DataPreparation(SparkSession spark){
        this.spark = spark;
    }

    public void startPrepareInsectStream() throws Exception{
        Dataset<Row> insectTableStream = fileStream(spark,insectSource, DataSchema.getFindingSchema())
                .withWatermark("DateAndTime","2 minutes");
        Dataset<Row> insectPopulation = insectTableStream
                .groupBy(
                        insectTableStream.col("InsectType"),
                        functions.window(functions.col("DateAndTime"), "2 minutes").as("TimeStamp")
                )
                .agg(

                        functions.count("*").as("Count"),
                        functions.avg("probability")
                )
                .withColumn("Date",functions.col("TimeStamp.end"))
                .withColumn("Accuracy",functions.format_number(functions.col("avg(probability)"),2))
                .select(
                        functions.col("InsectType"),
                        functions.col("Count"),
                        functions.col("Accuracy"),
                        functions.col("Date")
                );






        insectPopulation.writeStream()
                .format("delta")
                .outputMode("complete")
                .option("path",insectPopulationTablePath)
                .option("checkpointLocation", insectPopulationTablePathCheckpoint)
                .start()
                .processAllAvailable();
    }

}
