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
 
package org.apache.xtable.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.FileStats;
import org.apache.xtable.model.stat.Range;

public class TestDeltaParquetFooterStatsReader {
  @TempDir java.nio.file.Path tempDir;

  private final DeltaParquetFooterStatsReader reader =
      new DeltaParquetFooterStatsReader(new Configuration());

  @Test
  void readsStatsFromFooter() throws Exception {
    MessageType parquetSchema =
        Types.buildMessage()
            // "id" is written under its column mapping physical name
            .required(PrimitiveTypeName.INT64)
            .named("col-1")
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 9))
            .named("price")
            .required(PrimitiveTypeName.INT96)
            .named("legacy_ts")
            .requiredGroup()
            .required(PrimitiveTypeName.INT32)
            .named("x")
            .named("s")
            .named("record");
    String path = tempDir.resolve("data.parquet").toString();
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new Path(path)).withType(parquetSchema).build()) {
      SimpleGroupFactory groups = new SimpleGroupFactory(parquetSchema);
      writer.write(row(groups, 5L, "b", 1050, 7));
      writer.write(row(groups, 1L, null, 99, 3));
      writer.write(row(groups, 9L, "a", 2000, 5));
    }

    InternalField id = field("id", null, InternalType.LONG, "col-1");
    InternalField name = field("name", null, InternalType.STRING, null);
    InternalField price =
        field(
            "price",
            null,
            InternalType.DECIMAL,
            null,
            Collections.singletonMap(InternalSchema.MetadataKey.DECIMAL_SCALE, 2));
    InternalField legacyTs = field("legacy_ts", null, InternalType.TIMESTAMP, null);
    InternalField x = field("x", "s", InternalType.INT, null);
    InternalField s =
        InternalField.builder()
            .name("s")
            .schema(
                InternalSchema.builder()
                    .name("struct")
                    .dataType(InternalType.RECORD)
                    .fields(Collections.singletonList(x))
                    .build())
            .build();

    FileStats stats = reader.readStats(path, Arrays.asList(id, name, price, legacyTs, s, x));

    assertEquals(3, stats.getNumRecords());
    Map<String, ColumnStat> statsByPath =
        stats.getColumnStats().stream()
            .collect(Collectors.toMap(stat -> stat.getField().getPath(), Function.identity()));
    assertEquals(Range.vector(1L, 9L), statsByPath.get("id").getRange());
    assertEquals(Range.vector("a", "b"), statsByPath.get("name").getRange());
    assertEquals(1, statsByPath.get("name").getNumNulls());
    assertEquals(
        Range.vector(new BigDecimal("0.99"), new BigDecimal("20.00")),
        statsByPath.get("price").getRange());
    assertEquals(Range.vector(3, 7), statsByPath.get("s.x").getRange());
    assertEquals(4, statsByPath.size(), "INT96 and struct fields should have no stats");
  }

  @Test
  void limitsColumnStatsToFirstColumnsInFile() throws Exception {
    MessageType parquetSchema =
        Types.buildMessage()
            .required(PrimitiveTypeName.INT64)
            .named("a")
            .required(PrimitiveTypeName.INT64)
            .named("b")
            .required(PrimitiveTypeName.INT64)
            .named("c")
            .named("record");
    String path = tempDir.resolve("wide.parquet").toString();
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new Path(path)).withType(parquetSchema).build()) {
      SimpleGroupFactory groups = new SimpleGroupFactory(parquetSchema);
      writer.write(groups.newGroup().append("a", 1L).append("b", 2L).append("c", 3L));
      writer.write(groups.newGroup().append("a", 4L).append("b", 5L).append("c", 6L));
    }
    // the partition column is not stored in the file, so it must not use up the limit
    List<InternalField> fields =
        Arrays.asList(
            field("dt", null, InternalType.DATE, null),
            field("a", null, InternalType.LONG, null),
            field("b", null, InternalType.LONG, null),
            field("c", null, InternalType.LONG, null));
    Configuration conf = new Configuration();

    FileStats limited = new DeltaParquetFooterStatsReader(conf, 2).readStats(path, fields);
    assertEquals(2, limited.getNumRecords());
    assertEquals(
        Arrays.asList("a", "b"),
        limited.getColumnStats().stream()
            .map(stat -> stat.getField().getPath())
            .collect(Collectors.toList()));

    FileStats countOnly = new DeltaParquetFooterStatsReader(conf, 0).readStats(path, fields);
    assertEquals(2, countOnly.getNumRecords());
    assertTrue(countOnly.getColumnStats().isEmpty());

    FileStats unlimited = new DeltaParquetFooterStatsReader(conf, -1).readStats(path, fields);
    assertEquals(3, unlimited.getColumnStats().size());
  }

  @Test
  void missingFileYieldsEmptyStats() {
    FileStats stats =
        reader.readStats(
            tempDir.resolve("missing.parquet").toString(),
            Collections.singletonList(field("id", null, InternalType.LONG, null)));
    assertEquals(0, stats.getNumRecords());
    assertTrue(stats.getColumnStats().isEmpty());
  }

  private static Group row(SimpleGroupFactory groups, long id, String name, int price, int x) {
    Group group = groups.newGroup().append("col-1", id);
    if (name != null) {
      group.append("name", name);
    }
    group.append("price", price).append("legacy_ts", Binary.fromConstantByteArray(new byte[12]));
    group.addGroup("s").append("x", x);
    return group;
  }

  private static InternalField field(
      String name, String parentPath, InternalType type, String storageName) {
    return field(name, parentPath, type, storageName, null);
  }

  private static InternalField field(
      String name,
      String parentPath,
      InternalType type,
      String storageName,
      Map<InternalSchema.MetadataKey, Object> metadata) {
    return InternalField.builder()
        .name(name)
        .parentPath(parentPath)
        .storageName(storageName)
        .schema(
            InternalSchema.builder()
                .name(type.name())
                .dataType(type)
                .isNullable(true)
                .metadata(metadata)
                .build())
        .build();
  }
}
