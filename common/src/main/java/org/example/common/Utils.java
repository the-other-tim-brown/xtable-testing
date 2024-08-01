package org.example.common;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class Utils {
  public static final StructType STRUCT_TYPE = DataTypes.createStructType(Arrays.asList(
      DataTypes.createStructField("key", DataTypes.StringType, false),
      DataTypes.createStructField("partition_string", DataTypes.StringType, false),
      DataTypes.createStructField("time_millis", DataTypes.TimestampType, false),
      DataTypes.createStructField("value", DataTypes.StringType, true)));

  public static SparkSession getSparkSession(Path path) {
    SparkConf sparkConf = getSparkConf(path);
    SparkSession sparkSession =
        SparkSession.builder().config(sparkConf).getOrCreate();
    sparkSession
        .sparkContext()
        .hadoopConfiguration()
        .set("parquet.avro.write-old-list-structure", "false");
    return sparkSession;
  }

  private static SparkConf getSparkConf(Path tempDir) {
    return new SparkConf()
        .setAppName("xtable-testing")
        .set("spark.serializer", KryoSerializer.class.getName())
        .set("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.default_iceberg.type", "hadoop")
        .set("spark.sql.catalog.default_iceberg.warehouse", tempDir.toString())
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("parquet.avro.write-old-list-structure", "false")
        // Needed for ignoring not nullable constraints on nested columns in Delta.
        .set("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.default.parallelism", "1")
        .set("spark.sql.session.timeZone", "UTC")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .setMaster("local[4]");
  }

  public static List<Row> createRows() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    Row row1 = RowFactory.create("key1", "1", timestamp, "value1");
    Row row2 = RowFactory.create("key2", "1", timestamp, "value2");
    Row row3 = RowFactory.create("key3", "1", timestamp, "value3");
    return Arrays.asList(row1, row2, row3);
  }
}
