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
 
package org.apache.xtable.iceberg;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.ConversionTarget;

@Log4j2
public class IcebergConversionTarget implements ConversionTarget {
  private static final String METADATA_DIR_PATH = "/metadata/";
  // Expiring snapshots with file cleanup reads the manifest list of every retained snapshot one at
  // a time, which dominates the commit time of a table that commits often (~4k snapshots take
  // minutes on GCS). Snapshots are expired on the first commit of a sync run and then at most once
  // per this interval. A skipped expiry is only deferred: the next one expires and cleans up
  // everything past retention, so it leaves no orphan files. 0 expires on every commit.
  private static final Duration DEFAULT_SNAPSHOT_EXPIRY_INTERVAL =
      Duration.ofMinutes(Long.getLong("xtable.iceberg.snapshotExpiryIntervalMinutes", 60));
  private IcebergSchemaExtractor schemaExtractor;
  private IcebergSchemaSync schemaSync;
  private IcebergPartitionSpecExtractor partitionSpecExtractor;
  private IcebergPartitionSpecSync partitionSpecSync;
  private IcebergDataFileUpdatesSync dataFileUpdatesExtractor;
  private IcebergTableManager tableManager;
  private String basePath;
  private TableIdentifier tableIdentifier;
  private IcebergCatalogConfig catalogConfig;
  private Configuration configuration;
  private int snapshotRetentionInHours;
  private Duration snapshotExpiryInterval = DEFAULT_SNAPSHOT_EXPIRY_INTERVAL;
  // when this target last expired snapshots; null until its first commit
  private Instant lastSnapshotExpiry;
  private Transaction transaction;
  private Table table;
  private InternalTable internalTableState;
  private TableSyncMetadata tableSyncMetadata;

  public IcebergConversionTarget() {}

  IcebergConversionTarget(
      TargetTable targetTable,
      Configuration configuration,
      IcebergSchemaExtractor schemaExtractor,
      IcebergSchemaSync schemaSync,
      IcebergPartitionSpecExtractor partitionSpecExtractor,
      IcebergPartitionSpecSync partitionSpecSync,
      IcebergDataFileUpdatesSync dataFileUpdatesExtractor,
      IcebergTableManager tableManager) {
    _init(
        targetTable,
        configuration,
        schemaExtractor,
        schemaSync,
        partitionSpecExtractor,
        partitionSpecSync,
        dataFileUpdatesExtractor,
        tableManager);
  }

  private void _init(
      TargetTable targetTable,
      Configuration configuration,
      IcebergSchemaExtractor schemaExtractor,
      IcebergSchemaSync schemaSync,
      IcebergPartitionSpecExtractor partitionSpecExtractor,
      IcebergPartitionSpecSync partitionSpecSync,
      IcebergDataFileUpdatesSync dataFileUpdatesExtractor,
      IcebergTableManager tableManager) {
    this.schemaExtractor = schemaExtractor;
    this.schemaSync = schemaSync;
    this.partitionSpecExtractor = partitionSpecExtractor;
    this.partitionSpecSync = partitionSpecSync;
    this.dataFileUpdatesExtractor = dataFileUpdatesExtractor;
    String tableName = targetTable.getName();
    this.basePath = targetTable.getBasePath();
    this.configuration = configuration;
    this.snapshotRetentionInHours = (int) targetTable.getMetadataRetention().toHours();
    String[] namespace = targetTable.getNamespace();
    this.tableIdentifier =
        namespace == null
            ? TableIdentifier.of(tableName)
            : TableIdentifier.of(Namespace.of(namespace), tableName);
    this.tableManager = tableManager;
    this.catalogConfig = (IcebergCatalogConfig) targetTable.getCatalogConfig();

    if (tableManager.tableExists(catalogConfig, tableIdentifier, basePath)) {
      // Load the table state if it already exists
      this.table = tableManager.getTable(catalogConfig, tableIdentifier, basePath);
    }
    // Clear any corrupted state before using the conversion target
    rollbackCorruptCommits();
  }

  @Override
  public void init(TargetTable targetTable, Configuration configuration) {
    _init(
        targetTable,
        configuration,
        IcebergSchemaExtractor.getInstance(),
        IcebergSchemaSync.getInstance(),
        IcebergPartitionSpecExtractor.getInstance(),
        IcebergPartitionSpecSync.getInstance(),
        IcebergDataFileUpdatesSync.of(
            IcebergColumnStatsConverter.getInstance(),
            IcebergPartitionValueConverter.getInstance()),
        IcebergTableManager.of(configuration));
  }

  @Override
  public void beginSync(InternalTable internalTable) {
    initializeTableIfRequired(internalTable);
    transaction = table.newTransaction();
    internalTableState = internalTable;
  }

  private void initializeTableIfRequired(InternalTable internalTable) {
    if (table == null) {
      table =
          tableManager.getOrCreateTable(
              catalogConfig,
              tableIdentifier,
              basePath,
              schemaExtractor.toIceberg(internalTable.getReadSchema()),
              partitionSpecExtractor.toIceberg(
                  internalTable.getPartitioningFields(),
                  schemaExtractor.toIceberg(internalTable.getReadSchema())));
    }
  }

  /**
   * Create a name mapping from the given schema, add storage names in {@link
   * IcebergSchemaExtractor#getIdToStorageName()}, if any exist, to the name mapping, and apply the
   * updated mapping to the table.
   *
   * <p>This method should only be called when the name mapping is not set, or when field IDs are
   * provided in the source schema
   *
   * @param schema the {@link Schema} from which to create the name mapping
   */
  private void createAndSetNameMapping(Schema schema) {
    NameMapping mapping = MappingUtil.create(schema);
    MappedFields updatedMappedFields =
        updateNameMapping(mapping.asMappedFields(), schemaExtractor.getIdToStorageName());
    transaction
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(NameMapping.of(updatedMappedFields)))
        .commit();
  }

  private MappedFields updateNameMapping(
      MappedFields mapping, Map<Integer, String> idToStorageName) {
    if (mapping == null) {
      return null;
    }
    List<MappedField> fieldResults = new ArrayList<>();
    for (MappedField field : mapping.fields()) {
      Set<String> fieldNames = new HashSet<>(field.names());
      if (idToStorageName.containsKey(field.id())) {
        fieldNames.add(idToStorageName.get(field.id()));
      }
      MappedFields nestedMapping = updateNameMapping(field.nestedMapping(), idToStorageName);
      fieldResults.add(MappedField.of(field.id(), fieldNames, nestedMapping));
    }
    return MappedFields.of(fieldResults);
  }

  @Override
  public void syncSchema(InternalSchema schema) {
    Schema latestSchema = schemaExtractor.toIceberg(schema);
    if (!transaction.table().properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
      createAndSetNameMapping(latestSchema);
    }
    if (!transaction.table().schema().sameSchema(latestSchema)) {
      boolean hasFieldIds =
          schema.getAllFields().stream().anyMatch(field -> field.getFieldId() != null);
      if (hasFieldIds) {
        // If field IDs are provided in the source schema, manually update the name mapping to
        // ensure the IDs match the correct fields.
        createAndSetNameMapping(latestSchema);
        // There is no clean way to sync the schema with the provided field IDs using the
        // transaction API so we commit the current transaction and interact directly with
        // the operations API.
        transaction.commitTransaction();
        schemaSync.syncWithProvidedIds(latestSchema, table);
        // Start a new transaction for remaining operations
        table.refresh();
        transaction = table.newTransaction();
      } else {
        schemaSync.sync(transaction.table().schema(), latestSchema, transaction);
      }
    }
  }

  @Override
  public void syncPartitionSpec(List<InternalPartitionField> partitionSpec) {
    PartitionSpec latestPartitionSpec =
        partitionSpecExtractor.toIceberg(partitionSpec, transaction.table().schema());
    partitionSpecSync.sync(table.spec(), latestPartitionSpec, transaction);
  }

  @Override
  public void syncMetadata(TableSyncMetadata metadata) {
    tableSyncMetadata = metadata;

    UpdateProperties updateProperties = transaction.updateProperties();
    updateProperties.set(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
    if (!table.properties().containsKey(TableProperties.WRITE_DATA_LOCATION)) {
      // Required for a consistent write location when writing back to the table as Iceberg
      updateProperties.set(TableProperties.WRITE_DATA_LOCATION, basePath);
    }
    if (!Boolean.parseBoolean(
        table
            .properties()
            .getOrDefault(
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.FALSE.toString()))) {
      // Helps control the number of metadata files for frequently updated tables
      updateProperties.set(
          TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.TRUE.toString());
    }
    updateProperties.commit();
  }

  @Override
  public void syncFilesForSnapshot(List<PartitionFileGroup> partitionedDataFiles) {
    dataFileUpdatesExtractor.applySnapshot(
        table,
        internalTableState,
        transaction,
        partitionedDataFiles,
        transaction.table().schema(),
        transaction.table().spec(),
        tableSyncMetadata);
  }

  @Override
  public void syncFilesForDiff(InternalFilesDiff internalFilesDiff) {
    dataFileUpdatesExtractor.applyDiff(
        transaction,
        internalFilesDiff,
        transaction.table().schema(),
        transaction.table().spec(),
        tableSyncMetadata);
  }

  @Override
  public void completeSync() {
    Instant now = Instant.now();
    boolean expireSnapshots =
        lastSnapshotExpiry == null
            || !now.isBefore(lastSnapshotExpiry.plus(snapshotExpiryInterval));
    if (expireSnapshots) {
      transaction
          .expireSnapshots()
          .expireOlderThan(now.minus(snapshotRetentionInHours, ChronoUnit.HOURS).toEpochMilli())
          .deleteWith(this::safeDelete) // ensures that only metadata files are deleted
          .cleanExpiredFiles(true)
          .commit();
      log.info(
          "Expired snapshots older than {} hours for table {} in {} ms",
          snapshotRetentionInHours,
          tableIdentifier,
          Duration.between(now, Instant.now()).toMillis());
    }
    transaction.commitTransaction();
    if (expireSnapshots) {
      lastSnapshotExpiry = now;
    }
    transaction = null;
    internalTableState = null;
    tableSyncMetadata = null;
  }

  @VisibleForTesting
  void setSnapshotExpiryInterval(Duration snapshotExpiryInterval) {
    this.snapshotExpiryInterval = snapshotExpiryInterval;
  }

  private void safeDelete(String file) {
    if (file.startsWith(new Path(basePath) + METADATA_DIR_PATH)) {
      table.io().deleteFile(file);
    }
  }

  @Override
  public Optional<TableSyncMetadata> getTableMetadata() {
    if (table == null) {
      return Optional.empty();
    }
    return TableSyncMetadata.fromJson(table.properties().get(TableSyncMetadata.XTABLE_METADATA));
  }

  @Override
  public String getTableFormat() {
    return TableFormat.ICEBERG;
  }

  @Override
  public Optional<String> getTargetCommitIdentifier(String sourceIdentifier) {
    for (Snapshot snapshot : table.snapshots()) {
      Map<String, String> summary = snapshot.summary();
      String sourceMetadataJson = summary.get(TableSyncMetadata.XTABLE_METADATA);
      if (sourceMetadataJson == null) {
        continue;
      }

      try {
        Optional<TableSyncMetadata> optionalMetadata =
            TableSyncMetadata.fromJson(sourceMetadataJson);
        if (!optionalMetadata.isPresent()) {
          continue;
        }

        TableSyncMetadata metadata = optionalMetadata.get();
        if (sourceIdentifier.equals(metadata.getSourceIdentifier())) {
          return Optional.of(String.valueOf(snapshot.snapshotId()));
        }
      } catch (Exception e) {
        log.warn("Failed to parse parse snapshot metadata for {}", snapshot.snapshotId(), e);
      }
    }
    return Optional.empty();
  }

  private void rollbackCorruptCommits() {
    if (table == null) {
      // there is no existing table so exit early
      return;
    }
    if (table.currentSnapshot() == null) {
      return;
    }
    // Walk back from the current snapshot until we find one whose manifest list and every
    // manifest are readable. Two failure modes are handled:
    //   1. HadoopTables corruption: metadata.json is updated but the manifest list isn't written.
    //   2. Manifests deleted from storage while still referenced, e.g. by snapshot expiry in a
    //      concurrent sync of the same table, or by a bucket lifecycle rule.
    // Mode (1) only applies to HadoopTables (no catalog); mode (2) applies to any catalog.
    while (true) {
      Snapshot currentSnapshot = table.currentSnapshot();
      if (currentSnapshot == null) {
        return;
      }
      String corruptionReason = detectSnapshotCorruption(currentSnapshot);
      if (corruptionReason == null) {
        return;
      }
      Long parentId = currentSnapshot.parentId();
      if (parentId == null || table.snapshot(parentId) == null) {
        // Committing on top of this snapshot would carry the unreadable manifests forward, so fail
        // instead of producing a table that reports success but cannot be read.
        throw new IllegalStateException(
            String.format(
                "Iceberg table %s is unreadable at snapshot %d (%s) and has no earlier snapshot to"
                    + " roll back to. Delete the table's Iceberg metadata directory and resync to"
                    + " rebuild it from the source.",
                tableIdentifier, currentSnapshot.snapshotId(), corruptionReason));
      }
      log.warn(
          "Corrupt snapshot detected for table: {} snapshotId: {} ({}). Rolling back to previous"
              + " snapshot: {}; the next sync will be a full snapshot sync.",
          tableIdentifier,
          currentSnapshot.snapshotId(),
          corruptionReason,
          parentId);
      // Clear the last sync state alongside the rollback so the next sync round falls back to a
      // full snapshot sync instead of trying an incremental diff against a stale identifier.
      table.manageSnapshots().rollbackTo(parentId).commit();
      Transaction rollbackTxn = table.newTransaction();
      rollbackTxn.updateProperties().remove(TableSyncMetadata.XTABLE_METADATA).commit();
      rollbackTxn.commitTransaction();
    }
  }

  /**
   * Returns a non-null short description if the snapshot is unreadable, or {@code null} if it's
   * fully valid. Checks manifest-list readability (HadoopTables-only corruption) and leaf-manifest
   * existence (applies to any catalog when external deletion has occurred).
   */
  private String detectSnapshotCorruption(Snapshot snapshot) {
    List<ManifestFile> manifests;
    try {
      manifests = snapshot.allManifests(table.io());
    } catch (NotFoundException ex) {
      // Manifest list itself is missing — this is the HadoopTables-only failure mode the original
      // guard was written for.
      if (catalogConfig != null) {
        // Don't treat as corrupt for catalog-managed tables: the catalog should be the source of
        // truth, and a missing manifest list there indicates a different problem we shouldn't
        // silently rewrite around.
        throw ex;
      }
      return "manifest list missing: " + ex.getMessage();
    }
    Set<String> missingManifests = ConcurrentHashMap.newKeySet();
    Tasks.foreach(manifests)
        .executeWith(ThreadPools.getWorkerPool())
        .run(
            manifest -> {
              if (!table.io().newInputFile(manifest.path()).exists()) {
                missingManifests.add(manifest.path());
              }
            });
    return missingManifests.isEmpty()
        ? null
        : missingManifests.size()
            + " manifest(s) missing, e.g. "
            + missingManifests.iterator().next();
  }
}
