package org.example.hudiwriter;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.example.common.Utils.STRUCT_TYPE;
import static org.example.common.Utils.createRows;
import static org.example.common.Utils.getSparkSession;

public class TestHudiWriter {
  @TempDir Path tmpDir;

  @Test
  public void testFlow() throws Exception {
    Path path = tmpDir.resolve("test-table-1");
    SparkSession sparkSession = getSparkSession(path);
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

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

    validateHudiSchemaIdTracking(path.toString(), jsc);
    validateParquetSchemaFieldIdsSet(path.toString(), jsc);
  }

  private static HoodieTableMetaClient buildMetaClient(String tableBasePath, JavaSparkContext jsc) {
    return HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).setBasePath(tableBasePath).build();
  }

  private static void validateHudiSchemaIdTracking(String tableBasePath, JavaSparkContext jsc) throws Exception {
    HoodieTableMetaClient metaClient = buildMetaClient(tableBasePath, jsc);
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    Object hudiIdTracking = schemaResolver.getTableSchema(false).getProp("hudi_id_tracking");
    Assertions.assertNotNull(hudiIdTracking);
  }

  private static void validateParquetSchemaFieldIdsSet(String tableBasePath, JavaSparkContext jsc) throws Exception {
    HoodieTableMetaClient metaClient = buildMetaClient(tableBasePath, jsc);
    HoodieStorage storage = metaClient.getStorage();
    HoodieEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(false).build();
    FSUtils.getAllPartitionPaths(engineContext, metaClient, metadataConfig)
        .stream()
        .flatMap(partition -> {
          StoragePath partitionPath = new StoragePath(metaClient.getBasePath(), partition);
          try {
            return FSUtils.getAllDataFilesInPartition(storage, partitionPath).stream();
          } catch (IOException ex) {
            throw new UncheckedIOException(ex);
          }
        })
        .filter(pathInfo -> FSUtils.isBaseFile(pathInfo.getPath()))
        .map(pathInfo -> ParquetUtils.readMetadata(storage, pathInfo.getPath()))
        .forEach(parquetMetadata -> parquetMetadata.getFileMetaData().getSchema().getFields().forEach(type -> Assertions.assertNotNull(type.getId())));
  }


}
