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
 
package org.apache.xtable.spi.sync;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.xtable.annotations.Stable;
import org.apache.xtable.model.IncrementalTableChanges;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.storage.InternalFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.sync.ErrorDetails;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;
import org.apache.xtable.spi.extractor.StreamedSnapshot;

/** Provides the functionality to sync from the InternalTable format to the target format. */
@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableFormatSync {
  private static final TableFormatSync INSTANCE = new TableFormatSync();

  /**
   * When true, consecutive incremental changes with the same schema and partitioning are applied to
   * a target as one sync (one target commit) instead of one sync per source commit.
   */
  static final String MERGE_COMMITS_PROPERTY = "xtable.incremental.mergeCommits";

  /** Most source commits applied in one merged sync. */
  static final String MAX_COMMITS_PER_SYNC_PROPERTY = "xtable.incremental.maxCommitsPerSync";

  private static final int DEFAULT_MAX_COMMITS_PER_SYNC = 100;

  public static TableFormatSync getInstance() {
    return INSTANCE;
  }

  /**
   * Syncs the provided snapshot to the target table formats.
   *
   * @param conversionTargets the targets to sync with the snapshot
   * @param snapshot the snapshot to sync
   * @return the result of the sync process
   */
  @Stable
  public Map<String, SyncResult> syncSnapshot(
      Collection<ConversionTarget> conversionTargets, InternalSnapshot snapshot) {
    Instant startTime = Instant.now();
    Map<String, SyncResult> results = new HashMap<>();
    for (ConversionTarget conversionTarget : conversionTargets) {
      try {
        InternalTable internalTable = snapshot.getTable();
        results.put(
            conversionTarget.getTableFormat(),
            getSyncResult(
                conversionTarget,
                SyncMode.FULL,
                internalTable,
                target -> target.syncFilesForSnapshot(snapshot.getPartitionedDataFiles()),
                startTime,
                snapshot.getPendingCommits(),
                snapshot.getSourceIdentifier()));
      } catch (Exception e) {
        log.error("Failed to sync snapshot", e);
        results.put(
            conversionTarget.getTableFormat(), buildResultForError(SyncMode.FULL, startTime, e));
      }
    }
    return results;
  }

  /**
   * Syncs a snapshot whose data files are read through an iterator to a single target. The iterator
   * can be read only once, so only one target can be synced this way.
   *
   * @param conversionTarget the target to sync with the snapshot
   * @param streamed the snapshot and an iterator over its data files
   * @return the result of the sync process
   */
  public Map<String, SyncResult> syncSnapshot(
      ConversionTarget conversionTarget, StreamedSnapshot streamed) {
    Instant startTime = Instant.now();
    InternalSnapshot snapshot = streamed.getSnapshot();
    SyncResult result;
    try {
      result =
          getSyncResult(
              conversionTarget,
              SyncMode.FULL,
              snapshot.getTable(),
              target -> target.streamFilesForSnapshot(streamed.getFiles()),
              startTime,
              snapshot.getPendingCommits(),
              snapshot.getSourceIdentifier());
    } catch (Exception e) {
      log.error("Failed to sync snapshot", e);
      result = buildResultForError(SyncMode.FULL, startTime, e);
    }
    return Collections.singletonMap(conversionTarget.getTableFormat(), result);
  }

  /**
   * Syncs a set of changes to the target table formats.
   *
   * @param conversionTargetWithMetadata a map of conversion targets to their last sync metadata
   * @param changes the changes from the source table format that need to be applied
   * @return the results of trying to sync each change
   */
  @Stable
  public Map<String, List<SyncResult>> syncChanges(
      Map<ConversionTarget, TableSyncMetadata> conversionTargetWithMetadata,
      IncrementalTableChanges changes) {
    if (Boolean.getBoolean(MERGE_COMMITS_PROPERTY)) {
      return syncMergedChanges(conversionTargetWithMetadata, changes);
    }
    Map<String, List<SyncResult>> results = new HashMap<>();
    Set<ConversionTarget> conversionTargetsWithFailures = new HashSet<>();
    while (changes.getTableChanges().hasNext()) {
      TableChange change = changes.getTableChanges().next();
      Collection<ConversionTarget> conversionTargetsToSync =
          conversionTargetWithMetadata.entrySet().stream()
              .filter(
                  entry -> {
                    TableSyncMetadata metadata = entry.getValue();
                    return isChangeApplicableForLastSyncMetadata(change, metadata);
                  })
              .map(Map.Entry::getKey)
              .collect(Collectors.toList());
      for (ConversionTarget conversionTarget : conversionTargetsToSync) {
        if (conversionTargetsWithFailures.contains(conversionTarget)) {
          continue;
        }
        Instant startTime = Instant.now();
        List<SyncResult> resultsForFormat =
            results.computeIfAbsent(conversionTarget.getTableFormat(), key -> new ArrayList<>());
        try {
          resultsForFormat.add(
              getSyncResult(
                  conversionTarget,
                  SyncMode.INCREMENTAL,
                  change.getTableAsOfChange(),
                  target -> target.syncFilesForDiff(change.getFilesDiff()),
                  startTime,
                  changes.getPendingCommits(),
                  change.getSourceIdentifier()));
        } catch (Exception e) {
          log.error("Failed to sync table changes", e);
          resultsForFormat.add(buildResultForError(SyncMode.INCREMENTAL, startTime, e));
          conversionTargetsWithFailures.add(conversionTarget);
        }
      }
    }
    return results;
  }

  /**
   * Same as the per-commit path, but runs of consecutive changes that have the same schema,
   * partitioning and target set are applied as one sync. The files diff of a run is the net change
   * over its commits, and the run's last change supplies the table state and source identifier, so
   * the sync metadata ends at the same place as with per-commit syncs.
   */
  private Map<String, List<SyncResult>> syncMergedChanges(
      Map<ConversionTarget, TableSyncMetadata> conversionTargetWithMetadata,
      IncrementalTableChanges changes) {
    int maxCommitsPerSync =
        Math.max(
            1, Integer.getInteger(MAX_COMMITS_PER_SYNC_PROPERTY, DEFAULT_MAX_COMMITS_PER_SYNC));
    Map<String, List<SyncResult>> results = new HashMap<>();
    Set<ConversionTarget> conversionTargetsWithFailures = new HashSet<>();
    List<TableChange> group = new ArrayList<>();
    Set<ConversionTarget> groupTargets = new LinkedHashSet<>();
    while (changes.getTableChanges().hasNext()) {
      TableChange change = changes.getTableChanges().next();
      Set<ConversionTarget> targets = applicableTargets(conversionTargetWithMetadata, change);
      if (!group.isEmpty()
          && (group.size() >= maxCommitsPerSync
              || !targets.equals(groupTargets)
              || !sameLayout(group.get(group.size() - 1), change))) {
        syncGroup(group, groupTargets, changes, results, conversionTargetsWithFailures);
        group = new ArrayList<>();
      }
      if (group.isEmpty()) {
        groupTargets = targets;
      }
      group.add(change);
    }
    if (!group.isEmpty()) {
      syncGroup(group, groupTargets, changes, results, conversionTargetsWithFailures);
    }
    return results;
  }

  private void syncGroup(
      List<TableChange> group,
      Set<ConversionTarget> targets,
      IncrementalTableChanges changes,
      Map<String, List<SyncResult>> results,
      Set<ConversionTarget> conversionTargetsWithFailures) {
    InternalFilesDiff mergedDiff = mergeFilesDiffs(group);
    if (mergedDiff == null) {
      // A path was removed and added again inside the run; keep the commits apart.
      for (TableChange change : group) {
        syncChangeToTargets(
            change,
            change.getFilesDiff(),
            targets,
            changes,
            results,
            conversionTargetsWithFailures);
      }
      return;
    }
    TableChange last = group.get(group.size() - 1);
    if (group.size() > 1) {
      log.info(
          "Merged {} source commits ({} to {}) into one sync",
          group.size(),
          group.get(0).getSourceIdentifier(),
          last.getSourceIdentifier());
    }
    syncChangeToTargets(last, mergedDiff, targets, changes, results, conversionTargetsWithFailures);
  }

  private void syncChangeToTargets(
      TableChange change,
      InternalFilesDiff filesDiff,
      Set<ConversionTarget> targets,
      IncrementalTableChanges changes,
      Map<String, List<SyncResult>> results,
      Set<ConversionTarget> conversionTargetsWithFailures) {
    for (ConversionTarget conversionTarget : targets) {
      if (conversionTargetsWithFailures.contains(conversionTarget)) {
        continue;
      }
      Instant startTime = Instant.now();
      List<SyncResult> resultsForFormat =
          results.computeIfAbsent(conversionTarget.getTableFormat(), key -> new ArrayList<>());
      try {
        resultsForFormat.add(
            getSyncResult(
                conversionTarget,
                SyncMode.INCREMENTAL,
                change.getTableAsOfChange(),
                target -> target.syncFilesForDiff(filesDiff),
                startTime,
                changes.getPendingCommits(),
                change.getSourceIdentifier()));
      } catch (Exception e) {
        log.error("Failed to sync table changes", e);
        resultsForFormat.add(buildResultForError(SyncMode.INCREMENTAL, startTime, e));
        conversionTargetsWithFailures.add(conversionTarget);
      }
    }
  }

  private static Set<ConversionTarget> applicableTargets(
      Map<ConversionTarget, TableSyncMetadata> conversionTargetWithMetadata, TableChange change) {
    Set<ConversionTarget> targets = new LinkedHashSet<>();
    for (Map.Entry<ConversionTarget, TableSyncMetadata> entry :
        conversionTargetWithMetadata.entrySet()) {
      if (isChangeApplicableForLastSyncMetadata(change, entry.getValue())) {
        targets.add(entry.getKey());
      }
    }
    return targets;
  }

  private static boolean sameLayout(TableChange previous, TableChange next) {
    InternalTable a = previous.getTableAsOfChange();
    InternalTable b = next.getTableAsOfChange();
    return Objects.equals(a.getReadSchema(), b.getReadSchema())
        && Objects.equals(a.getPartitioningFields(), b.getPartitioningFields());
  }

  /**
   * Net files diff over a run of changes, matching files by physical path. A file added and later
   * removed inside the run is left out of both sets. Returns null when a path is removed and then
   * added again, which the run cannot express as one diff.
   */
  static InternalFilesDiff mergeFilesDiffs(List<TableChange> group) {
    if (group.size() == 1) {
      return group.get(0).getFilesDiff();
    }
    Map<String, InternalFile> added = new LinkedHashMap<>();
    Map<String, InternalFile> removed = new LinkedHashMap<>();
    for (TableChange change : group) {
      InternalFilesDiff diff = change.getFilesDiff();
      if (diff == null) {
        continue;
      }
      for (InternalFile file : diff.getFilesRemoved()) {
        String path = file.getPhysicalPath();
        if (added.remove(path) == null) {
          removed.put(path, file);
        }
      }
      for (InternalFile file : diff.getFilesAdded()) {
        String path = file.getPhysicalPath();
        if (removed.containsKey(path)) {
          return null;
        }
        added.put(path, file);
      }
    }
    return InternalFilesDiff.builder()
        .filesAdded(added.values())
        .filesRemoved(removed.values())
        .build();
  }

  private static boolean isChangeApplicableForLastSyncMetadata(
      TableChange change, TableSyncMetadata metadata) {
    return change
            .getTableAsOfChange()
            .getLatestCommitTime()
            .isAfter(metadata.getLastInstantSynced())
        || metadata
            .getInstantsToConsiderForNextSync()
            .contains(change.getTableAsOfChange().getLatestCommitTime());
  }

  private SyncResult getSyncResult(
      ConversionTarget conversionTarget,
      SyncMode mode,
      InternalTable tableState,
      SyncFiles fileSyncMethod,
      Instant startTime,
      List<Instant> pendingCommits,
      String sourceIdentifier) {
    // initialize the sync
    conversionTarget.beginSync(tableState);
    // Persist the latest commit time in table properties for incremental syncs
    // Syncing metadata must precede the following steps to ensure that the metadata is available
    // before committing
    TableSyncMetadata latestState =
        TableSyncMetadata.of(
            tableState.getLatestCommitTime(),
            pendingCommits,
            tableState.getTableFormat(),
            sourceIdentifier);
    conversionTarget.syncMetadata(latestState);
    // sync schema updates
    conversionTarget.syncSchema(tableState.getReadSchema());
    // sync partition updates
    conversionTarget.syncPartitionSpec(tableState.getPartitioningFields());
    // Update the files in the target table
    fileSyncMethod.sync(conversionTarget);
    conversionTarget.completeSync();

    return SyncResult.builder()
        .mode(mode)
        .tableFormatSyncStatus(SyncResult.SyncStatus.SUCCESS)
        .syncStartTime(startTime)
        .syncDuration(Duration.between(startTime, Instant.now()))
        .lastInstantSynced(tableState.getLatestCommitTime())
        .build();
  }

  @FunctionalInterface
  private interface SyncFiles {
    void sync(ConversionTarget conversionTarget);
  }

  private SyncResult buildResultForError(SyncMode mode, Instant startTime, Exception e) {
    return SyncResult.builder()
        .mode(mode)
        .tableFormatSyncStatus(
            SyncResult.SyncStatus.builder()
                .statusCode(SyncStatusCode.ERROR)
                .errorDetails(
                    ErrorDetails.builder()
                        .errorMessage(e.getMessage())
                        .errorDescription("Failed to sync " + mode.name())
                        .canRetryOnFailure(true)
                        .build())
                .build())
        .syncStartTime(startTime)
        .syncDuration(Duration.between(startTime, Instant.now()))
        .build();
  }
}
