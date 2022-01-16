package com.s4m.datatest;

import com.s4m.datatest.entity.Point;
import com.s4m.datatest.util.ConfigReader;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Properties;

import static com.s4m.datatest.Constants.SOURCE_PATH;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;


/**
 * Store Metrics
 */
public class PointMetrics {

    public static void main(String[] args) throws IOException {

        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();

        Properties properties = ConfigReader.readConfig("pointMetrics");

        Encoder<Point> pointEncoder = Encoders.bean(Point.class);

        StructType structType = new StructType();
        structType = structType.add("storeId", LongType, false);
        structType = structType.add("distanceFromStore", LongType, false);
        structType = structType.add("gpsLatitude", DoubleType, false);
        structType = structType.add("gpsLongitude", DoubleType, false);


        Dataset<Point> pointDataset = sparkSession
                .read()
                .format("csv")
                .option("header", true)
                .schema(structType)
                .load(properties.getProperty(SOURCE_PATH))
                .as(pointEncoder);

        WindowSpec window = Window
                .partitionBy(functions.col("storeId"))
                .orderBy("distanceFromStore");

        Dataset<Row> metricsDf = pointDataset
                .withColumn("row", functions.row_number().over(window))
                .filter(col("row").leq(10))
                .select("storeId", "distanceFromStore", "gpsLatitude", "gpsLongitude");

        metricsDf = metricsDf.groupBy("storeId").agg(avg("distanceFromStore"))
                .withColumnRenamed("avg(distanceFromStore)", "AverageDistance");

        metricsDf.show(false);

    }
}
