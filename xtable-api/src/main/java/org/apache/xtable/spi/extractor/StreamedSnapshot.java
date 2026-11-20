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
 
package org.apache.xtable.spi.extractor;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import org.apache.xtable.annotations.Evolving;
import org.apache.xtable.model.InternalSnapshot;

/**
 * The current snapshot of a source table whose data files are read through {@link #getFiles()}
 * instead of being held in {@link InternalSnapshot#getPartitionedDataFiles()}, so a full sync of a
 * table with tens of millions of files does not need them all in memory at once.
 */
@Evolving
@Value
@Builder
public class StreamedSnapshot {
  // The snapshot without its data files
  @NonNull InternalSnapshot snapshot;
  // The data files of the same snapshot; can be read once
  @NonNull DataFileIterator files;
}
