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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.spark.sql.delta.actions.AddFile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.FileStats;

/** Delta Standalone stats extractor - delegates to {@link DeltaStatsUtils} for shared logic. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaStatsExtractor {
  private static final DeltaStatsExtractor INSTANCE = new DeltaStatsExtractor();
  // Column stats taken from the Delta log's own stats are limited to the first N columns (-1: all).
  // Converted stats are held in memory for every file until the commit, so a full sync of a table
  // with millions of files can set this to bound memory; row counts are always kept.
  private static final int MAX_COLUMNS = Integer.getInteger("xtable.delta.statsMaxColumns", -1);

  private final DeltaParquetFooterStatsReader footerStatsReader =
      new DeltaParquetFooterStatsReader(new Configuration());

  public static DeltaStatsExtractor getInstance() {
    return INSTANCE;
  }

  /**
   * Converts XTable column statistics to Delta format JSON.
   *
   * @param schema the table schema
   * @param numRecords the number of records
   * @param columnStats the column statistics
   * @return JSON string in Delta format
   * @throws JsonProcessingException if serialization fails
   */
  public String convertStatsToDeltaFormat(
      InternalSchema schema, long numRecords, List<ColumnStat> columnStats)
      throws JsonProcessingException {
    return DeltaStatsUtils.convertStatsToDeltaFormat(schema, numRecords, columnStats);
  }

  /**
   * Extracts column statistics from Delta AddFile.
   *
   * @param addFile the Delta AddFile action
   * @param fields the fields to extract stats for
   * @return FileStats containing column statistics
   */
  public FileStats getColumnStatsForFile(AddFile addFile, List<InternalField> fields) {
    return limitColumnStats(
        DeltaStatsUtils.parseColumnStatsFromJson(addFile.stats(), fields), MAX_COLUMNS);
  }

  /**
   * Keeps the column stats of the first {@code maxColumns} columns; the record count is always
   * kept. A negative value keeps all columns.
   */
  @VisibleForTesting
  static FileStats limitColumnStats(FileStats stats, int maxColumns) {
    if (maxColumns < 0
        || stats.getColumnStats() == null
        || stats.getColumnStats().size() <= maxColumns) {
      return stats;
    }
    return stats.toBuilder()
        .columnStats(new ArrayList<>(stats.getColumnStats().subList(0, maxColumns)))
        .build();
  }

  /**
   * Extracts column statistics from Delta AddFile, falling back to the Parquet footer of the data
   * file when the AddFile carries no stats.
   *
   * @param addFile the Delta AddFile action
   * @param fullPath the absolute path to the data file
   * @param fields the fields to extract stats for
   * @return FileStats containing column statistics
   */
  public FileStats getColumnStatsForFile(
      AddFile addFile, String fullPath, List<InternalField> fields) {
    if (StringUtils.isNotEmpty(addFile.stats())) {
      return getColumnStatsForFile(addFile, fields);
    }
    return footerStatsReader.readStats(fullPath, fields);
  }

  /**
   * Returns unsupported stats discovered during parsing.
   *
   * @return set of unsupported stat names
   */
  @VisibleForTesting
  Set<String> getUnsupportedStats() {
    return DeltaStatsUtils.getUnsupportedStats();
  }
}
