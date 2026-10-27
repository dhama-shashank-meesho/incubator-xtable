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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.FileStats;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.parquet.ParquetMetadataExtractor;

/**
 * Reads file stats from a Parquet footer for Delta AddFile actions whose {@code stats} JSON is
 * missing (e.g. written with stats collection disabled). Without this, such files are synced with a
 * record count of 0 and no column stats.
 *
 * <p>Only stats that convert unambiguously to the internal representation are kept. A column whose
 * stats are absent or in a form that would need further interpretation (INT96, non-string binary,
 * non-micros timestamps) is skipped rather than risk writing wrong bounds to the target. A failure
 * to read stats never fails the sync; the record count is preserved whenever the footer is
 * readable.
 *
 * <p>Column stats are kept for at most the first {@code xtable.delta.footerStatsMaxColumns}
 * (default 100, Iceberg's default for inferred metrics) leaf columns of the file in schema order;
 * -1 keeps all. On wide tables stats for every column dominate memory during a sync and bloat the
 * target's manifests, while query engines rarely prune on columns that far into the schema.
 */
@Log4j2
class DeltaParquetFooterStatsReader {
  static final String MAX_COLUMNS_PROPERTY = "xtable.delta.footerStatsMaxColumns";
  private static final int DEFAULT_MAX_COLUMNS = 100;
  private static final ParquetMetadataExtractor PARQUET_METADATA_EXTRACTOR =
      ParquetMetadataExtractor.getInstance();

  private final Configuration conf;
  private final int maxColumns;

  DeltaParquetFooterStatsReader(Configuration conf) {
    this(conf, Integer.getInteger(MAX_COLUMNS_PROPERTY, DEFAULT_MAX_COLUMNS));
    log.info(
        "Column stats read from Parquet footers are limited to the first {} columns ({})",
        maxColumns < 0 ? "all" : maxColumns,
        MAX_COLUMNS_PROPERTY);
  }

  DeltaParquetFooterStatsReader(Configuration conf, int maxColumns) {
    this.conf = conf;
    this.maxColumns = maxColumns;
  }

  FileStats readStats(String fullPath, List<InternalField> fields) {
    ParquetMetadata footer;
    try {
      footer = PARQUET_METADATA_EXTRACTOR.readParquetMetadata(conf, new Path(fullPath));
    } catch (Exception e) {
      log.warn(
          "Failed to read Parquet footer for {}; file will be synced without stats: {}",
          fullPath,
          e.getMessage());
      return FileStats.builder().columnStats(Collections.emptyList()).numRecords(0).build();
    }
    long numRecords = footer.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
    List<ColumnStat> columnStats;
    try {
      columnStats = getColumnStats(footer, fields);
    } catch (Exception e) {
      log.warn(
          "Failed to read column stats from Parquet footer for {}; keeping record count only: {}",
          fullPath,
          e.getMessage());
      columnStats = Collections.emptyList();
    }
    return FileStats.builder().columnStats(columnStats).numRecords(numRecords).build();
  }

  private List<ColumnStat> getColumnStats(ParquetMetadata footer, List<InternalField> fields) {
    Map<String, List<ColumnChunkMetaData>> chunksByPath =
        footer.getBlocks().stream()
            .flatMap(block -> block.getColumns().stream())
            .collect(Collectors.groupingBy(chunk -> chunk.getPath().toDotString()));
    Map<String, String> physicalPathByLogicalPath = getPhysicalPaths(fields);
    List<ColumnStat> columnStats = new ArrayList<>();
    int columnsSeen = 0;
    for (InternalField field : fields) {
      if (field.getSchema().getFields() != null) {
        continue;
      }
      List<ColumnChunkMetaData> chunks =
          chunksByPath.get(physicalPathByLogicalPath.get(field.getPath()));
      if (chunks == null || chunks.isEmpty()) {
        // not stored in the file (e.g. a partition column), so it does not count toward the limit
        continue;
      }
      if (maxColumns >= 0 && columnsSeen >= maxColumns) {
        break;
      }
      columnsSeen++;
      ColumnStat stat = mergeChunks(field, chunks);
      if (stat != null) {
        columnStats.add(stat);
      }
    }
    return columnStats;
  }

  /**
   * Maps each field's logical path to the path of its column in the data file, which differs when
   * Delta column mapping is enabled. Relies on parents preceding their children in {@code fields},
   * as produced by {@code InternalSchema#getAllFields}.
   */
  private static Map<String, String> getPhysicalPaths(List<InternalField> fields) {
    Map<String, String> physicalPaths = new HashMap<>();
    for (InternalField field : fields) {
      String name = field.getStorageName() != null ? field.getStorageName() : field.getName();
      String parentPath = field.getParentPath();
      if (parentPath == null || parentPath.isEmpty()) {
        physicalPaths.put(field.getPath(), name);
      } else if (physicalPaths.containsKey(parentPath)) {
        physicalPaths.put(field.getPath(), physicalPaths.get(parentPath) + "." + name);
      }
    }
    return physicalPaths;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ColumnStat mergeChunks(InternalField field, List<ColumnChunkMetaData> chunks) {
    PrimitiveType primitiveType = chunks.get(0).getPrimitiveType();
    if (!isSupported(primitiveType)) {
      return null;
    }
    long numNulls = 0;
    long numValues = 0;
    long totalSize = 0;
    Comparable min = null;
    Comparable max = null;
    for (ColumnChunkMetaData chunk : chunks) {
      Statistics<?> statistics = chunk.getStatistics();
      if (statistics == null || statistics.isEmpty()) {
        // without stats for every chunk the null count and bounds would be incomplete
        return null;
      }
      numNulls += statistics.getNumNulls();
      numValues += chunk.getValueCount();
      totalSize += chunk.getTotalSize();
      if (!statistics.hasNonNullValue()) {
        continue;
      }
      Object chunkMin = convert(primitiveType, statistics.genericGetMin());
      Object chunkMax = convert(primitiveType, statistics.genericGetMax());
      if (!(chunkMin instanceof Comparable) || !(chunkMax instanceof Comparable)) {
        return null;
      }
      if (min == null || ((Comparable) chunkMin).compareTo(min) < 0) {
        min = (Comparable) chunkMin;
      }
      if (max == null || ((Comparable) chunkMax).compareTo(max) > 0) {
        max = (Comparable) chunkMax;
      }
    }
    return ColumnStat.builder()
        .field(field)
        .numValues(numValues)
        .numNulls(numNulls)
        .totalSize(totalSize)
        .range(Range.vector(min, max))
        .build();
  }

  private static boolean isSupported(PrimitiveType primitiveType) {
    LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();
    if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
        || annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      return true;
    }
    if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      // internal timestamps are micros; millis or nanos bounds would be misread
      return ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) annotation).getUnit()
          == LogicalTypeAnnotation.TimeUnit.MICROS;
    }
    switch (primitiveType.getPrimitiveTypeName()) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return true;
      default:
        // INT96, plain BINARY and FIXED_LEN_BYTE_ARRAY need interpretation the target can't verify
        return false;
    }
  }

  private static Object convert(PrimitiveType primitiveType, Object value) {
    LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();
    if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) annotation).getScale();
      if (value instanceof Number) {
        return new BigDecimal(BigInteger.valueOf(((Number) value).longValue()), scale);
      }
      if (value instanceof Binary) {
        return new BigDecimal(new BigInteger(((Binary) value).getBytes()), scale);
      }
      return null;
    }
    if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      return value instanceof Binary
          ? new String(((Binary) value).getBytes(), StandardCharsets.UTF_8)
          : null;
    }
    return value instanceof Binary ? null : value;
  }
}
