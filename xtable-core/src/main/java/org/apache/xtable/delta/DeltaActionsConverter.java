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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;

import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;
import org.apache.spark.sql.delta.actions.RemoveFile;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.ReadException;
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
  // A batch that is still running after this long logs its progress, and again at this interval.
  private static final String PROGRESS_LOG_INTERVAL_SECONDS =
      "xtable.delta.statsProgressLogIntervalSeconds";

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
   *
   * <p>Files without Delta stats have their Parquet footer read, which dominates the time on large
   * tables, so a batch running longer than {@code xtable.delta.statsProgressLogIntervalSeconds}
   * logs its progress periodically and a summary at the end.
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
    return convertAddActionsToInternalDataFiles(
        addFiles,
        tableBasePath,
        fileFormat,
        partitionFields,
        fields,
        includeColumnStats,
        partitionExtractor,
        fileStatsExtractor,
        TimeUnit.SECONDS.toMillis(Long.getLong(PROGRESS_LOG_INTERVAL_SECONDS, 30)));
  }

  @VisibleForTesting
  List<InternalDataFile> convertAddActionsToInternalDataFiles(
      List<AddFile> addFiles,
      String tableBasePath,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      List<InternalField> fields,
      boolean includeColumnStats,
      DeltaPartitionExtractor partitionExtractor,
      DeltaStatsExtractor fileStatsExtractor,
      long progressLogIntervalMillis) {
    long startMillis = System.currentTimeMillis();
    BatchProgress progress = new BatchProgress(tableBasePath, addFiles, startMillis);
    List<CompletableFuture<InternalDataFile>> futures = new ArrayList<>(addFiles.size());
    for (AddFile addFile : addFiles) {
      boolean readsFooter = StringUtils.isEmpty(addFile.stats());
      futures.add(
          CompletableFuture.supplyAsync(
              () -> {
                InternalDataFile dataFile =
                    convertAddActionToInternalDataFile(
                        addFile,
                        tableBasePath,
                        fileFormat,
                        partitionFields,
                        fields,
                        includeColumnStats,
                        partitionExtractor,
                        fileStatsExtractor);
                progress.recordConverted(readsFooter, dataFile.getRecordCount());
                return dataFile;
              },
              EXECUTOR));
    }
    List<InternalDataFile> dataFiles = new ArrayList<>(addFiles.size());
    long nextProgressLogMillis = startMillis + progressLogIntervalMillis;
    try {
      for (CompletableFuture<InternalDataFile> future : futures) {
        while (true) {
          long nowMillis = System.currentTimeMillis();
          if (nowMillis >= nextProgressLogMillis) {
            progress.logProgress();
            nextProgressLogMillis = nowMillis + progressLogIntervalMillis;
          }
          try {
            long waitMillis = Math.max(1, nextProgressLogMillis - System.currentTimeMillis());
            dataFiles.add(future.get(waitMillis, TimeUnit.MILLISECONDS));
            break;
          } catch (TimeoutException e) {
            // still running; log progress on the next pass
          }
        }
      }
    } catch (ExecutionException e) {
      futures.forEach(future -> future.cancel(true));
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      if (e.getCause() instanceof Error) {
        throw (Error) e.getCause();
      }
      throw new ReadException("Failed to convert Delta add actions for " + tableBasePath, e);
    } catch (InterruptedException e) {
      futures.forEach(future -> future.cancel(true));
      Thread.currentThread().interrupt();
      throw new ReadException("Interrupted converting Delta add actions for " + tableBasePath, e);
    }
    progress.logSummary(progressLogIntervalMillis);
    return dataFiles;
  }

  /** Counts for one batch of AddFile actions; updated by the converter threads. */
  private static class BatchProgress {
    private final String tableBasePath;
    private final List<AddFile> addFiles;
    private final long startMillis;
    private final long total;
    private final long withoutStats;
    private final AtomicLong convertedFromDeltaStats = new AtomicLong();
    private final AtomicLong convertedFromFooter = new AtomicLong();
    private final AtomicLong footerZeroRecords = new AtomicLong();

    BatchProgress(String tableBasePath, List<AddFile> addFiles, long startMillis) {
      this.tableBasePath = tableBasePath;
      this.addFiles = addFiles;
      this.startMillis = startMillis;
      this.total = addFiles.size();
      this.withoutStats = addFiles.stream().filter(f -> StringUtils.isEmpty(f.stats())).count();
    }

    void recordConverted(boolean readFooter, long recordCount) {
      if (readFooter) {
        convertedFromFooter.incrementAndGet();
        if (recordCount == 0) {
          footerZeroRecords.incrementAndGet();
        }
      } else {
        convertedFromDeltaStats.incrementAndGet();
      }
    }

    void logProgress() {
      long fromStats = convertedFromDeltaStats.get();
      long fromFooter = convertedFromFooter.get();
      long done = fromStats + fromFooter;
      log.info(
          "Delta add actions for {}: {}/{} converted ({}%) after {} s; Delta stats {}/{}, Parquet footers {}/{} ({} returned 0 rows)",
          tableBasePath,
          done,
          total,
          total == 0 ? 100 : done * 100 / total,
          (System.currentTimeMillis() - startMillis) / 1000,
          fromStats,
          total - withoutStats,
          fromFooter,
          withoutStats,
          footerZeroRecords.get());
    }

    void logSummary(long quietBelowMillis) {
      long elapsedMillis = System.currentTimeMillis() - startMillis;
      if (withoutStats == 0 && elapsedMillis < quietBelowMillis) {
        // fast batches with Delta stats for every file stay silent; one runs per incremental commit
        return;
      }
      long partitionsWithoutStats =
          withoutStats == 0
              ? 0
              : addFiles.stream()
                  .filter(f -> StringUtils.isEmpty(f.stats()))
                  .map(f -> String.valueOf(f.partitionValues()))
                  .distinct()
                  .count();
      String message =
          "Converted {} Delta add actions for {} in {} ms: {} used Delta stats, {} were read from Parquet footers across {} partition(s), {} footer reads returned 0 rows";
      Object[] args = {
        total,
        tableBasePath,
        elapsedMillis,
        total - withoutStats,
        withoutStats,
        partitionsWithoutStats,
        footerZeroRecords.get()
      };
      if (footerZeroRecords.get() > 0) {
        // 0 rows from a footer means the read failed or the file is empty; see the earlier WARN
        log.warn(message, args);
      } else {
        log.info(message, args);
      }
    }
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

  // Path.toString, not toUri().toString: the URI form escapes characters such as a space to %20,
  // and data file paths built on it would name objects that do not exist.
  private static String tableBasePath(Snapshot snapshot) {
    return snapshot.deltaLog().dataPath().toString();
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
