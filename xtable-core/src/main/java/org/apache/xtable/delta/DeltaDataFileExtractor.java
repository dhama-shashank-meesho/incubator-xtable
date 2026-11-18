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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import lombok.Builder;

import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.spi.extractor.DataFileIterator;

/** DeltaDataFileExtractor lets the consumer iterate over partitions. */
@Builder
public class DeltaDataFileExtractor {
  // Add actions converted per batch during a full sync; bounds the raw actions held in memory.
  private static final int BATCH_SIZE =
      Math.max(1, Integer.getInteger("xtable.delta.fullSyncBatchSize", 200_000));

  @Builder.Default
  private final DeltaPartitionExtractor partitionExtractor = DeltaPartitionExtractor.getInstance();

  @Builder.Default
  private final DeltaStatsExtractor fileStatsExtractor = DeltaStatsExtractor.getInstance();

  @Builder.Default
  private final DeltaActionsConverter actionsConverter = DeltaActionsConverter.getInstance();

  /**
   * Initializes an iterator for Delta Lake files.
   *
   * @return Delta table file iterator
   */
  public DataFileIterator iterator(Snapshot deltaSnapshot, InternalSchema schema) {
    return new DeltaDataFileIterator(deltaSnapshot, schema, true, BATCH_SIZE);
  }

  DataFileIterator iterator(Snapshot deltaSnapshot, InternalSchema schema, int batchSize) {
    return new DeltaDataFileIterator(deltaSnapshot, schema, true, batchSize);
  }

  public class DeltaDataFileIterator implements DataFileIterator {
    private final Snapshot snapshot;
    private final FileFormat fileFormat;
    private final List<InternalField> fields;
    private final List<InternalPartitionField> partitionFields;
    private final boolean includeColumnStats;
    private final int batchSize;
    // Streams the add actions from Spark one partition at a time instead of collecting them all
    // on the driver, so only one batch of raw actions (stats JSON included) is held at once.
    private final Iterator<AddFile> addFiles;
    private Iterator<InternalDataFile> currentBatch = Collections.emptyIterator();

    private DeltaDataFileIterator(
        Snapshot snapshot, InternalSchema schema, boolean includeColumnStats, int batchSize) {
      this.snapshot = snapshot;
      this.fileFormat =
          actionsConverter.convertToFileFormat(snapshot.metadata().format().provider());
      this.fields = schema.getAllFields();
      this.partitionFields =
          partitionExtractor.convertFromDeltaPartitionFormat(
              schema, snapshot.metadata().partitionSchema());
      this.includeColumnStats = includeColumnStats;
      this.batchSize = batchSize;
      this.addFiles = snapshot.allFiles().toLocalIterator();
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean hasNext() {
      while (!currentBatch.hasNext() && addFiles.hasNext()) {
        List<AddFile> batch = new ArrayList<>(batchSize);
        while (batch.size() < batchSize && addFiles.hasNext()) {
          batch.add(addFiles.next());
        }
        currentBatch =
            actionsConverter
                .convertAddActionsToInternalDataFiles(
                    batch,
                    snapshot,
                    fileFormat,
                    partitionFields,
                    fields,
                    includeColumnStats,
                    partitionExtractor,
                    fileStatsExtractor)
                .iterator();
      }
      return currentBatch.hasNext();
    }

    @Override
    public InternalDataFile next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return currentBatch.next();
    }
  }
}
