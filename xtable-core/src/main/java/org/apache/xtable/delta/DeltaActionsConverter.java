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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;

import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;
import org.apache.spark.sql.delta.actions.RemoveFile;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.FileStats;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;

@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaActionsConverter {
  // Converting an AddFile without Delta stats reads its Parquet footer, one remote call per file,
  // so batches are converted concurrently.
  private static final int PARALLELISM =
      Integer.getInteger("xtable.delta.statsReadParallelism", 32);
  private static final ExecutorService EXECUTOR =
      Executors.newFixedThreadPool(
          PARALLELISM,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("delta-add-file-converter-%d")
              .build());

  private static final DeltaActionsConverter INSTANCE = new DeltaActionsConverter();

  public static DeltaActionsConverter getInstance() {
    return INSTANCE;
  }

  public InternalDataFile convertAddActionToInternalDataFile(
      AddFile addFile,
      Snapshot deltaSnapshot,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      List<InternalField> fields,
      boolean includeColumnStats,
      DeltaPartitionExtractor partitionExtractor,
      DeltaStatsExtractor fileStatsExtractor) {
    return convertAddActionToInternalDataFile(
        addFile,
        tableBasePath(deltaSnapshot),
        fileFormat,
        partitionFields,
        fields,
        includeColumnStats,
        partitionExtractor,
        fileStatsExtractor);
  }

  public InternalDataFile convertAddActionToInternalDataFile(
      AddFile addFile,
      String tableBasePath,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      List<InternalField> fields,
      boolean includeColumnStats,
      DeltaPartitionExtractor partitionExtractor,
      DeltaStatsExtractor fileStatsExtractor) {
    String fullPath = DeltaPathUtils.getFullPathToFile(tableBasePath, addFile.path());
    FileStats fileStats = fileStatsExtractor.getColumnStatsForFile(addFile, fullPath, fields);
    List<ColumnStat> columnStats =
        includeColumnStats ? fileStats.getColumnStats() : Collections.emptyList();
    long recordCount = fileStats.getNumRecords();
    return InternalDataFile.builder()
        .physicalPath(fullPath)
        .fileFormat(fileFormat)
        .fileSizeBytes(addFile.size())
        .lastModified(addFile.modificationTime())
        .partitionValues(
            partitionExtractor.partitionValueExtraction(addFile.partitionValues(), partitionFields))
        .columnStats(columnStats)
        .recordCount(recordCount)
        .build();
  }

  public List<InternalDataFile> convertAddActionsToInternalDataFiles(
      List<AddFile> addFiles,
      Snapshot deltaSnapshot,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      List<InternalField> fields,
      boolean includeColumnStats,
      DeltaPartitionExtractor partitionExtractor,
      DeltaStatsExtractor fileStatsExtractor) {
    return convertAddActionsToInternalDataFiles(
        addFiles,
        tableBasePath(deltaSnapshot),
        fileFormat,
        partitionFields,
        fields,
        includeColumnStats,
        partitionExtractor,
        fileStatsExtractor);
  }

  /**
   * Converts the AddFile actions concurrently, preserving their order. Equivalent to calling {@link
   * #convertAddActionToInternalDataFile} for each one.
   */
  public List<InternalDataFile> convertAddActionsToInternalDataFiles(
      List<AddFile> addFiles,
      String tableBasePath,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      List<InternalField> fields,
      boolean includeColumnStats,
      DeltaPartitionExtractor partitionExtractor,
      DeltaStatsExtractor fileStatsExtractor) {
    long startMillis = System.currentTimeMillis();
    List<CompletableFuture<InternalDataFile>> futures = new ArrayList<>(addFiles.size());
    for (AddFile addFile : addFiles) {
      futures.add(
          CompletableFuture.supplyAsync(
              () ->
                  convertAddActionToInternalDataFile(
                      addFile,
                      tableBasePath,
                      fileFormat,
                      partitionFields,
                      fields,
                      includeColumnStats,
                      partitionExtractor,
                      fileStatsExtractor),
              EXECUTOR));
    }
    List<InternalDataFile> dataFiles;
    try {
      dataFiles = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
    } catch (CompletionException e) {
      futures.forEach(future -> future.cancel(true));
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw e;
    }
    long withoutStats = addFiles.stream().filter(f -> StringUtils.isEmpty(f.stats())).count();
    if (withoutStats > 0) {
      log.info(
          "Converted {} Delta add actions for {} in {} ms; {} had no Delta stats and were read from Parquet footers",
          addFiles.size(),
          tableBasePath,
          System.currentTimeMillis() - startMillis,
          withoutStats);
    }
    return dataFiles;
  }

  public InternalDataFile convertRemoveActionToInternalDataFile(
      RemoveFile removeFile,
      Snapshot deltaSnapshot,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      DeltaPartitionExtractor partitionExtractor) {
    return convertRemoveActionToInternalDataFile(
        removeFile, tableBasePath(deltaSnapshot), fileFormat, partitionFields, partitionExtractor);
  }

  public InternalDataFile convertRemoveActionToInternalDataFile(
      RemoveFile removeFile,
      String tableBasePath,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      DeltaPartitionExtractor partitionExtractor) {
    return InternalDataFile.builder()
        .physicalPath(DeltaPathUtils.getFullPathToFile(tableBasePath, removeFile.path()))
        .fileFormat(fileFormat)
        .partitionValues(
            partitionExtractor.partitionValueExtraction(
                removeFile.partitionValues(), partitionFields))
        .build();
  }

  public FileFormat convertToFileFormat(String provider) {
    if (provider.equals("parquet")) {
      return FileFormat.APACHE_PARQUET;
    } else if (provider.equals("orc")) {
      return FileFormat.APACHE_ORC;
    }
    throw new NotSupportedException(
        String.format("delta file format %s is not recognized", provider));
  }

  static String getFullPathToFile(Snapshot snapshot, String dataFilePath) {
    return DeltaPathUtils.getFullPathToFile(tableBasePath(snapshot), dataFilePath);
  }

  private static String tableBasePath(Snapshot snapshot) {
    return snapshot.deltaLog().dataPath().toUri().toString();
  }

  /**
   * Extracts the representation of the deletion vector information corresponding to an AddFile
   * action. Currently, this method extracts and returns the path to the data file for which a
   * deletion vector data is present.
   *
   * @param snapshot the commit snapshot
   * @param addFile the add file action
   * @return the deletion vector representation (path of data file), or null if no deletion vector
   *     is present
   */
  public String extractDeletionVectorFile(Snapshot snapshot, AddFile addFile) {
    return extractDeletionVectorFile(tableBasePath(snapshot), addFile);
  }

  public String extractDeletionVectorFile(String tableBasePath, AddFile addFile) {
    DeletionVectorDescriptor deletionVector = addFile.deletionVector();
    if (deletionVector == null) {
      return null;
    }

    String dataFilePath = addFile.path();
    return DeltaPathUtils.getFullPathToFile(tableBasePath, dataFilePath);
  }
}
