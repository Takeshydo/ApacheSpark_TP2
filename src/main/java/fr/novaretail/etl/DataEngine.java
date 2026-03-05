package fr.novaretail.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;


import java.util.Properties;

public class DataEngine {

    public static void pipeline(){
        System.setProperty("hadoop.home.dir", "C:\\Hadoop\\hadoop-3.3.6");
        System.load("C:\\Hadoop\\hadoop-3.3.6\\bin\\hadoop.dll");

        SparkSession spark = SparkSession.builder()
                .appName("spark")
                .config("spark.master", "local[*]")
                .getOrCreate();


        String url ="jdbc:mysql://localhost:3306/novaretail_legacy";
        String user = "root";
        String password ="";

        System.out.println("=========================================");
        System.out.println("CONNECTION DB MYSQL");
        System.out.println("=========================================");

        Properties connectionProp = new Properties();
        connectionProp.put("user", user);
        connectionProp.put("password", password);
        connectionProp.put("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> dfMySQL = spark.read().jdbc(url, "customer_transaction", connectionProp);
        Dataset<Row> dfFilter = dfMySQL.select("transaction_id","customer_name","customer_age", "country", "purchase_amount", "clearence_level");


        System.out.println("=========================================");
        System.out.println("ETAPE DU MAPPING");
        System.out.println("=========================================");

        Dataset<Row> dfMap = dfFilter
                .select("country", "purchase_amount", "customer_name")
                        .filter(dfMySQL.col("country").isNotNull());

        System.out.println("=========================================");
        System.out.println("ETAPE DU REDUCE");
        System.out.println("=========================================");

        Dataset<Row> dfReduce = dfMap
                .orderBy(col("country"), col("purchase_amount").desc());

        String pathExportJSON = "archives_purchase";

        dfReduce.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("country")
                .json(pathExportJSON);


        System.out.println("=========================================");
        System.out.println("EXPORTE REALISEE : " + pathExportJSON);
        System.out.println("=========================================");

        spark.stop();
    }
}
