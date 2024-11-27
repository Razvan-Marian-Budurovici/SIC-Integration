package General.Bronze;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataSchema {

    public static StructType getSchema() {

        StructType findingsSchema = new StructType(new StructField[]{
                DataTypes.createStructField("x", DataTypes.DoubleType, false),
                DataTypes.createStructField("y", DataTypes.DoubleType, false),
                DataTypes.createStructField("width", DataTypes.DoubleType, false),
                DataTypes.createStructField("height", DataTypes.DoubleType, false),
                DataTypes.createStructField("stretched", DataTypes.BooleanType, false),
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("slug", DataTypes.StringType, false),
                DataTypes.createStructField("colorProfile", DataTypes.StringType, false),
                DataTypes.createStructField("probability", DataTypes.DoubleType, false),
                DataTypes.createStructField("type", DataTypes.StringType, false)
        });

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("imageId", DataTypes.StringType, false),
                DataTypes.createStructField("version", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("colorProfile", DataTypes.StringType, false),
                DataTypes.createStructField("findings", DataTypes.createArrayType(findingsSchema), false)
        });

        return schema;
    }

}
