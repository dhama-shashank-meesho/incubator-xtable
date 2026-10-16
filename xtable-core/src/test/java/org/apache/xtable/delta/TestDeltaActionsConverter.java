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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;

import scala.Option;

import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.stat.FileStats;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;

class TestDeltaActionsConverter {

  @Test
  void extractDeletionVector() throws URISyntaxException {
    DeltaActionsConverter actionsConverter = DeltaActionsConverter.getInstance();

    int size = 123;
    long time = 234L;
    boolean dataChange = true;
    String stats = "";
    String filePath = "https://container.blob.core.windows.net/tablepath/file_path";
    Snapshot snapshot = Mockito.mock(Snapshot.class);
    DeltaLog deltaLog = Mockito.mock(DeltaLog.class);
    Mockito.when(snapshot.deltaLog()).thenReturn(deltaLog);
    Mockito.when(deltaLog.dataPath())
        .thenReturn(new Path("https://container.blob.core.windows.net/tablepath"));

    DeletionVectorDescriptor deletionVector = null;
    AddFile addFileAction =
        new AddFile(filePath, null, size, time, dataChange, stats, null, deletionVector);
    Assertions.assertNull(actionsConverter.extractDeletionVectorFile(snapshot, addFileAction));

    deletionVector =
        DeletionVectorDescriptor.onDiskWithAbsolutePath(
            filePath, size, 42, Option.empty(), Option.empty());

    addFileAction =
        new AddFile(filePath, null, size, time, dataChange, stats, null, deletionVector);

    Assertions.assertEquals(
        filePath, actionsConverter.extractDeletionVectorFile(snapshot, addFileAction));
  }

  @Test
  void convertAddActionsPreservesOrder() {
    DeltaStatsExtractor statsExtractor = Mockito.mock(DeltaStatsExtractor.class);
    Mockito.when(statsExtractor.getColumnStatsForFile(any(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              String path = invocation.getArgument(1);
              long records = Long.parseLong(path.substring(path.lastIndexOf('-') + 1));
              return FileStats.builder()
                  .columnStats(Collections.emptyList())
                  .numRecords(records)
                  .build();
            });
    List<AddFile> addFiles =
        IntStream.range(0, 200)
            .mapToObj(i -> new AddFile("file-" + i, null, 10, 1L, true, "", null, null))
            .collect(Collectors.toList());

    List<InternalDataFile> dataFiles =
        DeltaActionsConverter.getInstance()
            .convertAddActionsToInternalDataFiles(
                addFiles,
                "gs://bucket/table",
                FileFormat.APACHE_PARQUET,
                Collections.emptyList(),
                Collections.emptyList(),
                true,
                partitionExtractor(),
                statsExtractor);

    Assertions.assertEquals(200, dataFiles.size());
    for (int i = 0; i < 200; i++) {
      Assertions.assertEquals("gs://bucket/table/file-" + i, dataFiles.get(i).getPhysicalPath());
      Assertions.assertEquals(i, dataFiles.get(i).getRecordCount());
    }
  }

  @Test
  void convertAddActionsPropagatesFailure() {
    DeltaStatsExtractor statsExtractor = Mockito.mock(DeltaStatsExtractor.class);
    Mockito.when(statsExtractor.getColumnStatsForFile(any(), anyString(), any()))
        .thenThrow(new ReadException("boom"));
    List<AddFile> addFiles =
        Collections.singletonList(new AddFile("file-0", null, 10, 1L, true, "", null, null));

    Assertions.assertThrows(
        ReadException.class,
        () ->
            DeltaActionsConverter.getInstance()
                .convertAddActionsToInternalDataFiles(
                    addFiles,
                    "gs://bucket/table",
                    FileFormat.APACHE_PARQUET,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    true,
                    partitionExtractor(),
                    statsExtractor));
  }

  private static DeltaPartitionExtractor partitionExtractor() {
    DeltaPartitionExtractor partitionExtractor = Mockito.mock(DeltaPartitionExtractor.class);
    Mockito.when(partitionExtractor.partitionValueExtraction(any(), any()))
        .thenReturn(Collections.emptyList());
    return partitionExtractor;
  }
}
