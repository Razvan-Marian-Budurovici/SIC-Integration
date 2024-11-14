package WorkPackage.SmartInsectCounting.Silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static General.Silver.DataOrganization.fileStream;

public class DataNormalisation {

    String fileSource = "/opt/bitnami/spark/WP_1/Bronze/Data",
        insectFindingTablePath = "/opt/bitnami/spark/WP_1/Silver/Data/insectFindingTable",
        yellowCardTablePath = "/opt/bitnami/spark/WP_1/Silver/Data/yellowCardTable",
        greenhouseTablePath = "/opt/bitnami/spark/WP_1/Silver/Data/greenhouseTable";
    SparkSession spark;

    public DataNormalisation(SparkSession spark){
        this.spark = spark;
    }

    public void startTableUpdateStream(){
        Dataset<Row> readStream = fileStream(spark,fileSource);
        Dataset<Row> insectFindingEntry = readStream
                .select(readStream.col("imageID").as("CardID"), functions.explode(readStream.col("findings")).as("finding"))
                .select("finding.id", "CardID", "finding.probability").withColumnRenamed("id","InsectID")
                .withColumn("InsectType",functions.col("InsectID"))
                .groupBy("InsectID")
                .count();

        insectFindingEntry.write().format("delta").mode("append").save(insectFindingTablePath);

        Dataset<Row> yellowCard = readStream
                .select(readStream.col("imageID").as("CardID"))
                .withColumn("GreenhouseID",functions.col("CardID"))
                .withColumn("Location",functions.col("CardID"))
                .withColumn("Date",functions.current_date())
                .withColumn("Time",functions.current_timestamp());

        yellowCard.write().format("delta").mode("append").save(yellowCardTablePath);
    }
}
