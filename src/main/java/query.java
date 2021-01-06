import java.io.IOException;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class query {
    public static void main(String[] args) {
        SparkSession spark;
        FileSystem fs;
        try {
            SparkSession.Builder builder = SparkSession.builder();
            spark = builder.appName("DatalakeSparkQuery").getOrCreate();
            fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        } catch (IOException e) {
            throw new RuntimeException("Failed to init spark session");
        }
        Dataset<Row> usersDF = spark.read().format("avro").load("sqoop_landing/users");
        Dataset<Row> userInfoDF = spark.read().format("avro").load("sqoop_landing/userinfo");
        Dataset<Row> accountsDF = spark.read().format("avro").load("sqoop_landing/accounts");
//        usersDF.limit(10).show();
//        accountsDF.limit(10).show();
        Dataset<Row> output = userInfoDF.join(
                accountsDF,
                userInfoDF.col("userid").equalTo(accountsDF.col("userid")),
                "left_outer")
                .drop(accountsDF.col("userid"));
//        output.limit(100).repartition(1)
//                .write().mode("overwrite").format("csv").option("header",true).save("sqoop_landing/output");
        output.limit(100).write().saveAsTable("testdb.output");

    }
}
