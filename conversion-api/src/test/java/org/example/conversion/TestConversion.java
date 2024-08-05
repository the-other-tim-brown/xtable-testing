package org.example.conversion;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.PerTableConfig;
import org.apache.xtable.conversion.PerTableConfigImpl;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.hudi.HudiSourceConfigImpl;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    provider.init(new Configuration(), Collections.emptyMap());
    PerTableConfig perTableConfig = PerTableConfigImpl.builder()
        .tableBasePath(path.toString())
        .tableName("table_1")
        .targetTableFormats(Arrays.asList("HUDI", "ICEBERG"))
        .syncMode(SyncMode.INCREMENTAL)
        .build();
    conversionController.sync(perTableConfig, provider).forEach((format, result) -> {
      assertEquals(SyncResult.SyncStatusCode.SUCCESS, result.getStatus().getStatusCode());
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
    options.put("hoodie.avro.write.support.class", "org.apache.xtable.hudi.extensions.HoodieAvroWriteSupportWithFieldIds");
    options.put("hoodie.client.init.callback.classes", "org.apache.xtable.hudi.extensions.AddFieldIdsClientInitCallback");
    options.put("hoodie.datasource.write.row.writer.enable", "false");

    sparkSession.createDataset(createRows(), RowEncoder.apply(STRUCT_TYPE)).write().format("hudi")
        .options(options).mode(SaveMode.Append).save(path.toString());

    ConversionController conversionController = new ConversionController(new Configuration());
    ConversionSourceProvider<HoodieInstant> provider = new HudiConversionSourceProvider();
    provider.init(new Configuration(), Collections.emptyMap());
    PerTableConfig perTableConfig = PerTableConfigImpl.builder()
        .hudiSourceConfig(HudiSourceConfigImpl.builder()
            .partitionFieldSpecConfig("partition_string:VALUE")
            .build())
        .tableBasePath(path.toString())
        .tableName("table_1")
        .targetTableFormats(Arrays.asList("DELTA", "ICEBERG"))
        .syncMode(SyncMode.INCREMENTAL)
        .build();
    conversionController.sync(perTableConfig, provider).forEach((format, result) -> {
      assertEquals(SyncResult.SyncStatusCode.SUCCESS, result.getStatus().getStatusCode());
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
      provider.init(new Configuration(), Collections.emptyMap());
      PerTableConfig perTableConfig = PerTableConfigImpl.builder()
          .tableBasePath(path.toString())
          .tableName(tableName)
          .targetTableFormats(Arrays.asList("HUDI", "DELTA"))
          .syncMode(SyncMode.INCREMENTAL)
          .build();
      conversionController.sync(perTableConfig, provider).forEach((format, result) -> {
        assertEquals(SyncResult.SyncStatusCode.SUCCESS, result.getStatus().getStatusCode());
      });
    }
  }
}
