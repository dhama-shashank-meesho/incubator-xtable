/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.hex;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.spark.sql.delta.DeltaLog;

import scala.collection.JavaConverters;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;

/**
 * Iceberg tables written by Spark, converted to Delta in place and read back. Each case checks that
 * the Delta table returns the same rows as the Iceberg table, and that filters return the same rows
 * with Delta data skipping on, in UTC and in a non-UTC session.
 */
public class ITIcebergToDeltaConversion {
  @TempDir public static Path tempDir;
  private static final String CATALOG = "default_iceberg.db.";
  private static final List<String> SESSION_ZONES = Arrays.asList("UTC", "Asia/Kolkata");
  private static SparkSession spark;
  private static ConversionController conversionController;

  @BeforeAll
  public static void setupOnce() {
    SparkConf conf =
        HudiTestUtil.getSparkConf(tempDir)
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + "io.delta.sql.DeltaSparkSessionExtension");
    spark = SparkSession.builder().config(conf).getOrCreate();
    conversionController = new ConversionController(spark.sparkContext().hadoopConfiguration());
  }

  @AfterAll
  public static void teardown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void typesNullsAndIdentityPartitions() {
    String table = "types_identity";
    spark.sql(
        "CREATE TABLE "
            + CATALOG
            + table
            + " (id BIGINT, s STRING, d DATE, ts TIMESTAMP, ntz TIMESTAMP_NTZ, dec DECIMAL(38,10),"
            + " dec2 DECIMAL(9,2), f FLOAT, db DOUBLE, b BOOLEAN, bin BINARY,"
            + " st STRUCT<a: INT, b: STRING>, arr ARRAY<STRING>, m MAP<STRING, INT>)"
            + " USING iceberg PARTITIONED BY (d, s) TBLPROPERTIES ('format-version'='2')");
    insertTypedRows(table, 0);
    insertTypedRows(table, 100);
    syncAndCheck(table, SyncMode.FULL);
    assertEquals(Arrays.asList("d", "s"), deltaPartitionColumns(table));
    checkTypedFilters(table);

    insertTypedRows(table, 200);
    syncAndCheck(table, SyncMode.INCREMENTAL);
    checkTypedFilters(table);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"days(ts)", "hours(ts)", "months(ts)", "years(ts)", "bucket(4, id)", "b, i"})
  public void partitionTransforms(String partitioning) {
    // Delta derives partition filters for days/hours from CAST(ts AS DATE) and
    // DATE_FORMAT(ts, 'yyyy-MM-dd-HH'), which Spark evaluates in the session time zone, while the
    // partition values are UTC as Iceberg defines them: such tables must be read in UTC sessions.
    List<String> zones =
        partitioning.startsWith("days") || partitioning.startsWith("hours")
            ? Collections.singletonList("UTC")
            : SESSION_ZONES;
    String table = "transform_" + partitioning.replaceAll("[^a-z0-9]+", "_");
    spark.sql(
        "CREATE TABLE "
            + CATALOG
            + table
            + " (id BIGINT, s STRING, ts TIMESTAMP, b BOOLEAN, i INT) USING iceberg"
            + " PARTITIONED BY ("
            + partitioning
            + ") TBLPROPERTIES ('format-version'='2')");
    insertSimpleRows(table, 0, 40);
    syncAndCheck(table, SyncMode.FULL);
    checkFilters(
        table,
        zones,
        "ts > TIMESTAMP '2026-11-15 10:30:00.500+00:00'",
        "id = 7",
        "s = 'k3'",
        "b AND i < 5");

    insertSimpleRows(table, 40, 40);
    syncAndCheck(table, SyncMode.INCREMENTAL);
    checkFilters(
        table,
        zones,
        "ts <= TIMESTAMP '2026-11-16 02:00:00+05:30'",
        "ts >= TIMESTAMP '2026-11-16 00:00:00+05:30'",
        "ts BETWEEN TIMESTAMP '2026-11-15 23:00:00+05:30' AND TIMESTAMP '2026-11-16 01:00:00+05:30'",
        "id >= 60");
  }

  @Test
  public void truncatePartitioningIsRejected() {
    String table = "truncate_rejected";
    spark.sql(
        "CREATE TABLE "
            + CATALOG
            + table
            + " (id BIGINT, s STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY (truncate(2, s))"
            + " TBLPROPERTIES ('format-version'='2')");
    insertSimpleRows(table, 0, 10);
    assertThrows(NotSupportedException.class, () -> sync(table, SyncMode.FULL));
    assertFalse(
        new java.io.File(basePath(table), "_delta_log/00000000000000000000.json").exists(),
        "a rejected sync must not create a Delta table");
  }

  @Test
  public void copyOnWriteRowLevelOperations() {
    String table = "cow_ops";
    spark.sql(
        "CREATE TABLE "
            + CATALOG
            + table
            + " (id BIGINT, s STRING, d DATE, v DOUBLE) USING iceberg PARTITIONED BY (d)"
            + " TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='copy-on-write',"
            + " 'write.update.mode'='copy-on-write', 'write.merge.mode'='copy-on-write')");
    spark.sql(
        "INSERT INTO "
            + CATALOG
            + table
            + " SELECT id, concat('s', id % 7), date_add(DATE '2026-11-01', CAST(id % 5 AS INT)),"
            + " id * 1.5 FROM range(0, 500)");
    syncAndCheck(table, SyncMode.FULL);

    spark.sql("DELETE FROM " + CATALOG + table + " WHERE id % 10 = 0");
    syncAndCheck(table, SyncMode.INCREMENTAL);
    spark.sql("UPDATE " + CATALOG + table + " SET v = -1 WHERE id % 7 = 1");
    syncAndCheck(table, SyncMode.INCREMENTAL);
    spark.sql(
        "INSERT INTO "
            + CATALOG
            + table
            + " SELECT id, concat('m', id), DATE '2026-11-03', 9.0 FROM range(500, 520)");
    spark.sql("UPDATE " + CATALOG + table + " SET v = 9.5, s = 'u' WHERE id BETWEEN 490 AND 510");
    syncAndCheck(table, SyncMode.INCREMENTAL);
    spark.sql(
        "INSERT OVERWRITE "
            + CATALOG
            + table
            + " PARTITION (d = DATE '2026-11-02') SELECT id, 'over', 0.0 FROM range(1000, 1010)");
    syncAndCheck(table, SyncMode.INCREMENTAL);
    spark.sql("CALL default_iceberg.system.rewrite_data_files(table => 'db." + table + "')");
    syncAndCheck(table, SyncMode.INCREMENTAL);
    checkFilters(table, "v = -1", "d = DATE '2026-11-02'", "id BETWEEN 100 AND 200");
  }

  @Test
  public void mergeOnReadDeletes() {
    String table = "mor_deletes";
    spark.sql(
        "CREATE TABLE "
            + CATALOG
            + table
            + " (id BIGINT, s STRING) USING iceberg TBLPROPERTIES ('format-version'='2',"
            + " 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read')");
    spark.sql("INSERT INTO " + CATALOG + table + " SELECT id, concat('s', id) FROM range(0, 100)");
    syncAndCheck(table, SyncMode.FULL);
    long rowsBeforeDelete = deltaRowCount(table);
    spark.sql("DELETE FROM " + CATALOG + table + " WHERE id < 10");
    assertThrows(NotSupportedException.class, () -> sync(table, SyncMode.INCREMENTAL));
    assertThrows(NotSupportedException.class, () -> sync(table, SyncMode.FULL));
    assertEquals(rowsBeforeDelete, deltaRowCount(table), "failed syncs must not change Delta");

    // documented remedy: rewrite the data files the deletes apply to, then sync
    spark.sql(
        "CALL default_iceberg.system.rewrite_data_files(table => 'db."
            + table
            + "', options => map('delete-file-threshold', '1'))");
    syncAndCheck(table, SyncMode.FULL);
    spark.sql(
        "INSERT INTO " + CATALOG + table + " SELECT id, concat('s', id) FROM range(100, 110)");
    syncAndCheck(table, SyncMode.INCREMENTAL);
  }

  @Test
  public void schemaEvolution() {
    String table = "schema_evolution";
    spark.sql(
        "CREATE TABLE "
            + CATALOG
            + table
            + " (id BIGINT, i INT, old_name STRING, gone STRING) USING iceberg"
            + " TBLPROPERTIES ('format-version'='2')");
    spark.sql(
        "INSERT INTO "
            + CATALOG
            + table
            + " SELECT id, CAST(id AS INT), concat('o', id), 'x' FROM range(0, 20)");
    syncAndCheck(table, SyncMode.FULL);

    spark.sql("ALTER TABLE " + CATALOG + table + " ADD COLUMN added DOUBLE");
    spark.sql(
        "INSERT INTO "
            + CATALOG
            + table
            + " SELECT id, CAST(id AS INT), concat('o', id), 'x', id * 2.0 FROM range(20, 30)");
    syncAndCheck(table, SyncMode.INCREMENTAL);

    spark.sql("ALTER TABLE " + CATALOG + table + " DROP COLUMN gone");
    spark.sql(
        "INSERT INTO "
            + CATALOG
            + table
            + " SELECT id, CAST(id AS INT), 'o', 2.0 FROM range(30, 35)");
    syncAndCheck(table, SyncMode.INCREMENTAL);

    long rows = deltaRowCount(table);
    spark.sql("ALTER TABLE " + CATALOG + table + " ALTER COLUMN i TYPE BIGINT");
    spark.sql(
        "INSERT INTO "
            + CATALOG
            + table
            + " SELECT id, id * 1000000000, concat('o', id), 1.0 FROM range(35, 40)");
    assertThrows(NotSupportedException.class, () -> sync(table, SyncMode.INCREMENTAL));
    assertThrows(NotSupportedException.class, () -> sync(table, SyncMode.FULL));
    assertEquals(rows, deltaRowCount(table), "a rejected sync must not change the Delta table");
  }

  @Test
  public void columnRenameAndDrop() {
    String table = "rename_drop";
    spark.sql(
        "CREATE TABLE "
            + CATALOG
            + table
            + " (id BIGINT, old_name STRING, gone STRING) USING iceberg"
            + " TBLPROPERTIES ('format-version'='2')");
    spark.sql(
        "INSERT INTO " + CATALOG + table + " SELECT id, concat('o', id), 'x' FROM range(0, 20)");
    syncAndCheck(table, SyncMode.FULL);
    long rows = deltaRowCount(table);
    spark.sql("ALTER TABLE " + CATALOG + table + " RENAME COLUMN old_name TO new_name");
    spark.sql(
        "INSERT INTO " + CATALOG + table + " SELECT id, concat('n', id), 'x' FROM range(20, 30)");
    assertThrows(NotSupportedException.class, () -> sync(table, SyncMode.INCREMENTAL));
    assertThrows(NotSupportedException.class, () -> sync(table, SyncMode.FULL));
    assertEquals(rows, deltaRowCount(table), "a rejected sync must not change the Delta table");
  }

  @Test
  public void partitionEvolution() {
    String table = "partition_evolution";
    spark.sql(
        "CREATE TABLE "
            + CATALOG
            + table
            + " (id BIGINT, s STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY (s)"
            + " TBLPROPERTIES ('format-version'='2')");
    insertSimpleRows(table, 0, 20);
    syncAndCheck(table, SyncMode.FULL);
    spark.sql("ALTER TABLE " + CATALOG + table + " ADD PARTITION FIELD days(ts)");
    insertSimpleRows(table, 20, 20);
    syncAndCheck(table, SyncMode.INCREMENTAL);
    checkFilters(
        table,
        "ts >= TIMESTAMP '2026-11-15 10:00:00+00:00'",
        "ts < TIMESTAMP '2026-11-15 12:00:00+00:00'",
        "s = 'k1'");
  }

  @Test
  public void expiredSnapshotsFallBackToSnapshotSync() {
    String table = "expired_snapshots";
    spark.sql(
        "CREATE TABLE "
            + CATALOG
            + table
            + " (id BIGINT, s STRING, ts TIMESTAMP) USING iceberg TBLPROPERTIES ('format-version'='2')");
    insertSimpleRows(table, 0, 10);
    syncAndCheck(table, SyncMode.FULL);
    insertSimpleRows(table, 10, 10);
    insertSimpleRows(table, 20, 10);
    spark.sql(
        "CALL default_iceberg.system.expire_snapshots(table => 'db."
            + table
            + "', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)");
    syncAndCheck(table, SyncMode.INCREMENTAL);
  }

  @Test
  public void emptyTable() {
    String table = "empty_table";
    spark.sql(
        "CREATE TABLE "
            + CATALOG
            + table
            + " (id BIGINT, d DATE) USING iceberg PARTITIONED BY (d)"
            + " TBLPROPERTIES ('format-version'='2')");
    spark.sql("INSERT INTO " + CATALOG + table + " SELECT id, DATE '2026-11-01' FROM range(0, 5)");
    spark.sql("DELETE FROM " + CATALOG + table + " WHERE true");
    syncAndCheck(table, SyncMode.FULL);
  }

  private void insertTypedRows(String table, int offset) {
    // nulls in every column, a null partition, characters that need escaping in partition paths,
    // NaN and infinity, sub-second timestamps on both sides of midnight UTC
    spark.sql(
        "INSERT INTO "
            + CATALOG
            + table
            + " SELECT id,"
            + " CASE WHEN id % 11 = 0 THEN NULL WHEN id % 5 = 0 THEN 'a/b c=%' ELSE concat('k', id % 4) END,"
            + " CASE WHEN id % 13 = 0 THEN NULL ELSE date_add(DATE '2026-11-14', CAST(id % 3 AS INT)) END,"
            + " CASE WHEN id % 17 = 0 THEN NULL ELSE TIMESTAMP '2026-11-15 18:29:59.250+00:00'"
            + "   + make_interval(0, 0, 0, 0, 0, CAST(id % 90 AS INT), id % 7 + 0.125) END,"
            + " CASE WHEN id % 19 = 0 THEN NULL ELSE TIMESTAMP_NTZ '2026-11-15 23:59:59.750'"
            + "   + make_interval(0, 0, 0, 0, 0, CAST(id % 30 AS INT), 0) END,"
            + " CAST(id AS DECIMAL(38,10)) / 7,"
            + " CAST(id % 1000 AS DECIMAL(9,2)) / 3,"
            + " CASE WHEN id % 23 = 0 THEN CAST('NaN' AS FLOAT) ELSE CAST(id AS FLOAT) / 2 END,"
            + " CASE WHEN id % 29 = 0 THEN CAST('Infinity' AS DOUBLE) WHEN id % 31 = 0 THEN NULL ELSE id / 3.0 END,"
            + " id % 2 = 0,"
            + " CAST(concat('bin', id) AS BINARY),"
            + " named_struct('a', CAST(id AS INT), 'b', concat('x', id)),"
            + " CASE WHEN id % 6 = 0 THEN NULL ELSE array(concat('e', id), 'f') END,"
            + " map('k', CAST(id AS INT))"
            + " FROM range("
            + offset
            + ", "
            + (offset + 100)
            + ")");
  }

  private void checkTypedFilters(String table) {
    checkFilters(
        table,
        "ts > TIMESTAMP '2026-11-15 18:30:30.500+00:00'",
        "ts < TIMESTAMP '2026-11-16 00:00:15+05:30'",
        "ntz > TIMESTAMP_NTZ '2026-11-16 00:10:00'",
        "dec > 10.5",
        "dec2 <= 5.33",
        "d = DATE '2026-11-15'",
        "s IS NULL",
        "s = 'a/b c=%'",
        "db IS NULL",
        "f > 20 AND NOT isnan(f)",
        "id BETWEEN 10 AND 60");
  }

  private void insertSimpleRows(String table, int offset, int count) {
    spark.sql(
        "INSERT INTO "
            + CATALOG
            + table
            + " SELECT id, concat('k', id % 5),"
            + " TIMESTAMP '2026-11-15 06:00:00.250+00:00' + make_interval(0, 0, 0, 0, CAST(id AS INT), 0, 0)"
            + (table.startsWith("transform_") ? ", id % 2 = 0, CAST(id % 10 AS INT)" : "")
            + " FROM range("
            + offset
            + ", "
            + (offset + count)
            + ")");
  }

  private void syncAndCheck(String table, SyncMode syncMode) {
    Map<String, SyncResult> results = sync(table, syncMode);
    SyncResult result = results.get(DELTA);
    if (result == null) {
      // nothing to sync, e.g. a schema change that wrote no new snapshot
      assertSameRows(table);
      return;
    }
    assertEquals(
        SyncResult.SyncStatus.SUCCESS,
        result.getTableFormatSyncStatus(),
        table + " " + syncMode + " sync failed: " + result.getTableFormatSyncStatus());
    assertSameRows(table);
  }

  private Map<String, SyncResult> sync(String table, SyncMode syncMode) {
    String basePath = basePath(table);
    SourceTable sourceTable =
        SourceTable.builder()
            .name(table)
            .formatName(ICEBERG)
            .basePath(basePath)
            .dataPath(basePath + "/data")
            .build();
    TargetTable targetTable =
        TargetTable.builder()
            .name(table)
            .formatName(DELTA)
            .basePath(basePath)
            .metadataRetention(Duration.ofDays(7))
            .build();
    ConversionConfig config =
        ConversionConfig.builder()
            .sourceTable(sourceTable)
            .targetTables(Collections.singletonList(targetTable))
            .syncMode(syncMode)
            .build();
    IcebergConversionSourceProvider provider = new IcebergConversionSourceProvider();
    provider.init(spark.sparkContext().hadoopConfiguration());
    return conversionController.sync(config, provider);
  }

  private void assertSameRows(String table) {
    Dataset<Row> iceberg = spark.table(CATALOG + table);
    Column[] columns = comparable(iceberg.schema());
    Dataset<Row> expected = iceberg.select(columns);
    Dataset<Row> actual =
        spark
            .read()
            .format("delta")
            .load(basePath(table))
            .select(iceberg.columns()[0], tail(iceberg.columns()))
            .select(columns);
    assertEquals(expected.count(), actual.count(), table + ": row count");
    assertEquals(0, expected.exceptAll(actual).count(), table + ": rows only in Iceberg");
    assertEquals(0, actual.exceptAll(expected).count(), table + ": rows only in Delta");
  }

  private long deltaRowCount(String table) {
    return spark.read().format("delta").load(basePath(table)).count();
  }

  private void checkFilters(String table, String... predicates) {
    checkFilters(table, SESSION_ZONES, predicates);
  }

  private void checkFilters(String table, List<String> zones, String... predicates) {
    List<String> mismatches = new java.util.ArrayList<>();
    String original = spark.conf().get("spark.sql.session.timeZone");
    try {
      for (String zone : zones) {
        spark.conf().set("spark.sql.session.timeZone", zone);
        for (String predicate : predicates) {
          long expected = spark.table(CATALOG + table).where(predicate).count();
          long actual = spark.read().format("delta").load(basePath(table)).where(predicate).count();
          System.out.println(
              "I2D-FILTER "
                  + table
                  + " ["
                  + predicate
                  + "] session="
                  + zone
                  + " iceberg="
                  + expected
                  + " delta="
                  + actual
                  + (expected == actual ? " OK" : " MISMATCH"));
          if (expected != actual) {
            mismatches.add(
                predicate + " in " + zone + ": iceberg " + expected + ", delta " + actual);
          }
        }
      }
    } finally {
      spark.conf().set("spark.sql.session.timeZone", original);
    }
    assertEquals(Collections.emptyList(), mismatches, table + ": filter results differ");
  }

  private List<String> deltaPartitionColumns(String table) {
    return JavaConverters.seqAsJavaList(
        DeltaLog.forTable(spark, basePath(table))
            .update(false, scala.Option.empty())
            .metadata()
            .partitionColumns());
  }

  private static Column[] comparable(StructType schema) {
    return Arrays.stream(schema.fields())
        .map(ITIcebergToDeltaConversion::comparable)
        .toArray(Column[]::new);
  }

  // set operations need hashable, comparable columns
  private static Column comparable(StructField field) {
    if (field.dataType() instanceof MapType
        || field.dataType() instanceof StructType
        || field.dataType() instanceof ArrayType) {
      return to_json(col(field.name())).alias(field.name());
    }
    if (field.dataType() instanceof BinaryType) {
      return hex(col(field.name())).alias(field.name());
    }
    return col(field.name());
  }

  private static String[] tail(String[] columns) {
    return Arrays.copyOfRange(columns, 1, columns.length);
  }

  private static String basePath(String table) {
    return tempDir.resolve("db").resolve(table).toString();
  }
}
