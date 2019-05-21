/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.distributedlog.restore.impl;

import io.atomix.cluster.MemberId;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreStrategy;
import io.zeebe.distributedlog.restore.log.LogReplicator;
import io.zeebe.distributedlog.restore.snapshot.SnapshotRestoreContext;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.state.SnapshotReplication;
import io.zeebe.logstreams.state.SnapshotRequester;
import io.zeebe.logstreams.state.StateStorage;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.slf4j.LoggerFactory;

public class SnapshotRestoreStrategy implements RestoreStrategy {

  private SnapshotRequester requester;
  private MemberId server;
  private final SnapshotRestoreContext restoreContext;
  private RestoreClient client;
  private int partitionId;
  private final LogStream logStream;
  private final LogReplicator logReplicator;
  private long backupPosition;
  private SnapshotReplication processorSnapshotReplicationConsumer;
  private SnapshotReplication exporterSnapshotReplicationConsumer;
  private StateStorage exporterStorage;
  private long latestLocalPosition;
  private StateStorage processorStorage;

  public SnapshotRestoreStrategy(
      RestoreClient client, int partitionId, LogStream logStream, LogReplicator logReplicator) {
    this.client = client;
    this.restoreContext = client.createSnapshotRestoreContext();
    this.partitionId = partitionId;
    this.logStream = logStream;
    this.logReplicator = logReplicator;
    initializeSnapshotRequester(client);
  }

  private void initializeSnapshotRequester(RestoreClient client) {
    exporterSnapshotReplicationConsumer =
        restoreContext.createExporterSnapshotReplicationConsumer(partitionId);
    processorSnapshotReplicationConsumer =
        restoreContext.createProcessorSnapshotReplicationConsumer(partitionId);
    processorStorage = restoreContext.getProcessorStateStorage(partitionId);
    exporterStorage = restoreContext.getExporterStateStorage(partitionId);

    this.requester =
        new SnapshotRequester(
            client,
            processorSnapshotReplicationConsumer,
            exporterSnapshotReplicationConsumer,
            processorStorage,
            exporterStorage);
  }

  @Override
  public CompletableFuture<Long> executeRestoreStrategy() {
    final CompletableFuture<Long> replicated = CompletableFuture.completedFuture(null);

    return replicated
        .thenCompose(nothing -> client.requestSnapshotInfo(server))
        .thenCompose(numSnapshots -> requester.getLatestSnapshotsFrom(server, numSnapshots > 1))
        .thenCompose(pos -> onSnapshotsReplicated());
  }

  private CompletableFuture<Long> onSnapshotsReplicated() {
    final long lastEventPosition =
        Math.max(
            latestLocalPosition,
            getValidSnapshotPosition()); // if exporter position is behind local logstream
    // logStream.delete(lastEventPosition); //TODO
    LoggerFactory.getLogger("Snapshot restore")
        .info("Snapshot replicated {}, backup position {}", lastEventPosition, backupPosition);
    if (lastEventPosition < backupPosition) {
      return logReplicator.replicate(server, lastEventPosition, backupPosition, true);
    } else {
      return logReplicator.replicate(
          server,
          lastEventPosition,
          lastEventPosition,
          true); // replicate one event for the snapshot to be useable
    }
  }

  private long getValidSnapshotPosition() {
    final Supplier<Long> exporterPositionSupplier =
        restoreContext.getExporterPositionSupplier(exporterStorage);
    final Supplier<Long> processedPositionSupplier =
        restoreContext.getProcessorPositionSupplier(partitionId, processorStorage);
    final long latestProcessedPosition = processedPositionSupplier.get();
    if (exporterPositionSupplier != null) {
      final long exporterPosition = exporterPositionSupplier.get();
      if (exporterPosition > 0) {
        return Math.min(latestProcessedPosition, exporterPosition);
      }
    }
    return latestProcessedPosition;
  }

  public void setBackupPosition(long backupPosition) {
    this.backupPosition = backupPosition;
  }

  public void setServer(MemberId server) {
    this.server = server;
  }

  public void setLatestLocalPosition(long latestLocalPosition) {
    this.latestLocalPosition = latestLocalPosition;
  }
}
