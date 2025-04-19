package org.example.conversion;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncStatusCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.xtable.hudi.HudiSourceConfig.PARTITION_FIELD_SPEC_CONFIG;
import static org.example.common.Utils.STRUCT_TYPE;
import static org.example.common.Utils.createRows;
import static org.example.common.Utils.getSparkSession;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestConversion {
  private SparkSession sparkSession;
  @TempDir Path tmpDir;

  @BeforeEach
  void setup() {
    sparkSession = getSparkSession(tmpDir);
  }

  @AfterEach
  void teardown() {
    sparkSession.close();
  }

  @Test
  void convertFromDelta() {
    Path path = tmpDir.resolve("test-table-delta");
    sparkSession.createDataset(createRows(), RowEncoder.apply(STRUCT_TYPE)).write().format("delta")
        .mode(SaveMode.Append).save(path.toString());

    ConversionController conversionController = new ConversionController(new Configuration());
    ConversionSourceProvider<Long> provider = new DeltaConversionSourceProvider();
    provider.init(new Configuration());
    String tableName = "table_1";
    ConversionConfig conversionConfig = ConversionConfig.builder()
        .sourceTable(SourceTable.builder()
            .basePath(path.toString())
            .name(tableName)
            .formatName("DELTA")
            .build())
        .targetTables(Arrays.asList(
            TargetTable.builder().basePath(path.toString()).formatName("HUDI").name(tableName).build(),
            TargetTable.builder().basePath(path.toString()).formatName("ICEBERG").name(tableName).build()))
        .syncMode(SyncMode.INCREMENTAL)
        .build();
    conversionController.sync(conversionConfig, provider).forEach((format, result) -> {
      assertEquals(SyncStatusCode.SUCCESS, result.getTableFormatSyncStatus().getStatusCode());
    });
  }

  @Test
  void convertFromHudi() {
    Path path = tmpDir.resolve("test-table-hudi");
    Map<String, String> options = new HashMap<>();
    options.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "key");
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "key");
    options.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_string");
    options.put(HoodieTableConfig.NAME.key(), "test-table1");
    options.put(HoodieWriteConfig.BASE_PATH.key(), path.toString());
    options.put("hoodie.datasource.write.row.writer.enable", "false");

    sparkSession.createDataset(createRows(), RowEncoder.apply(STRUCT_TYPE)).write().format("hudi")
        .options(options).mode(SaveMode.Append).save(path.toString());

    ConversionController conversionController = new ConversionController(new Configuration());
    ConversionSourceProvider<HoodieInstant> provider = new HudiConversionSourceProvider();
    provider.init(new Configuration());
    String tableName = "table_2";
    Properties sourceProperties = new Properties();
    sourceProperties.put(PARTITION_FIELD_SPEC_CONFIG, "partition_string:VALUE");
    ConversionConfig conversionConfig = ConversionConfig.builder()
        .sourceTable(SourceTable.builder()
            .basePath(path.toString())
            .name(tableName)
            .additionalProperties(sourceProperties)
            .formatName("HUDI")
            .build())
        .targetTables(Arrays.asList(
            TargetTable.builder().basePath(path.toString()).formatName("DELTA").name(tableName).build(),
            TargetTable.builder().basePath(path.toString()).formatName("ICEBERG").name(tableName).build()))
        .syncMode(SyncMode.INCREMENTAL)
        .build();
    conversionController.sync(conversionConfig, provider).forEach((format, result) -> {
      assertEquals(SyncStatusCode.SUCCESS, result.getTableFormatSyncStatus().getStatusCode());
    });
  }

  @Test
  void convertFromIceberg() throws Exception {
    String tableName = "table_1";
    Path path = tmpDir.resolve(tableName);
    try (HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), tmpDir.toString())) {
      // No namespace specified.
      TableIdentifier tableIdentifier = TableIdentifier.of(tableName);
      Schema schema = new Schema(Arrays.asList(
          Types.NestedField.of(1, false, "key", Types.StringType.get()),
          Types.NestedField.of(2, false, "partition_string", Types.StringType.get()),
          Types.NestedField.of(3, false, "time_millis", Types.TimestampType.withZone()),
          Types.NestedField.of(4, true, "value", Types.StringType.get())));
      hadoopCatalog.createTable(tableIdentifier, schema, PartitionSpec.unpartitioned());
      sparkSession.createDataset(createRows(), RowEncoder.apply(STRUCT_TYPE)).write().format("iceberg")
          .mode(SaveMode.Append).save(path.toString());

      ConversionController conversionController = new ConversionController(new Configuration());
      ConversionSourceProvider<Snapshot> provider = new IcebergConversionSourceProvider();
      provider.init(new Configuration());
      ConversionConfig conversionConfig = ConversionConfig.builder()
          .sourceTable(SourceTable.builder()
              .basePath(path.toString())
              .name(tableName)
              .formatName("ICEBERG")
              .build())
          .targetTables(Arrays.asList(
              TargetTable.builder().basePath(path.toString()).formatName("HUDI").name(tableName).build(),
              TargetTable.builder().basePath(path.toString()).formatName("DELTA").name(tableName).build()))
          .syncMode(SyncMode.INCREMENTAL)
          .build();
      conversionController.sync(conversionConfig, provider).forEach((format, result) -> {
        assertEquals(SyncStatusCode.SUCCESS, result.getTableFormatSyncStatus().getStatusCode());
      });
    }
  }
}
