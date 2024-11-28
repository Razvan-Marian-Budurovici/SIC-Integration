package General.Silver;

import General.Bronze.DataSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class DataOrganization {

    public static Dataset<Row> fileStream(SparkSession spark, String source, StructType schema){
        return spark.readStream().schema(schema).parquet(source);
    }

}
