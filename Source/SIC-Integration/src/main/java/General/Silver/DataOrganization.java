package General.Silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataOrganization {

    public static Dataset<Row> fileStream(SparkSession spark, String source){
        return spark.readStream().parquet(source);
    }

}
