/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.logstreams.restore;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.zeebe.broker.engine.EngineService;
import io.zeebe.broker.exporter.ExporterManagerService;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreClientFactory;
import io.zeebe.distributedlog.restore.RestoreServer;
import io.zeebe.engine.state.replication.StateReplication;
import io.zeebe.logstreams.state.SnapshotReplication;

public class BrokerRestoreFactory implements RestoreClientFactory {
  private final ClusterCommunicationService communicationService;
  private final ClusterEventService eventService;

  public BrokerRestoreFactory(
      ClusterCommunicationService communicationService, ClusterEventService eventService) {
    this.communicationService = communicationService;
    this.eventService = eventService;
  }

  @Override
  public RestoreClient createClient(int partitionId) {
    return new BrokerRestoreClient(
        communicationService,
        getLogReplicationTopic(partitionId),
        getRestoreInfoTopic(partitionId),
        getSnapshotRequestTopic(partitionId),
        getSnapshotInfoRequestTopic(partitionId));
  }

  public RestoreServer createServer(int partitionId) {
    return new BrokerRestoreServer(
        communicationService,
        getLogReplicationTopic(partitionId),
        getRestoreInfoTopic(partitionId),
        getSnapshotRequestTopic(partitionId),
        getSnapshotInfoRequestTopic(partitionId));
  }

  @Override
  public SnapshotReplication createProcessorSnapshotReplicationConsumer(int partitionId) {
    return new StateReplication(eventService, partitionId, EngineService.PROCESSOR_NAME);
  }

  @Override
  public SnapshotReplication createExporterSnapshotReplicationConsumer(int partitionId) {
    return new StateReplication(eventService, partitionId, ExporterManagerService.PROCESSOR_NAME);
  }

  private String getLogReplicationTopic(int partitionId) {
    return String.format("log-replication-%d", partitionId);
  }

  private String getRestoreInfoTopic(int partitionId) {
    return String.format("restore-info-%d", partitionId);
  }

  private String getSnapshotRequestTopic(int partitionId) {
    return String.format("snapshot-request-%d", partitionId);
  }

  private String getSnapshotInfoRequestTopic(int partitionId) {
    return String.format("snapshot-info-request-%d", partitionId);
  }
}
