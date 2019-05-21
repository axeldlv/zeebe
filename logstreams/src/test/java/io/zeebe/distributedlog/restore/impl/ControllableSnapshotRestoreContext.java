package io.zeebe.distributedlog.restore.impl;

import io.zeebe.distributedlog.restore.snapshot.SnapshotRestoreContext;
import io.zeebe.logstreams.state.SnapshotReplication;
import io.zeebe.logstreams.state.StateStorage;
import java.util.function.Supplier;

public class ControllableSnapshotRestoreContext implements SnapshotRestoreContext {

  private SnapshotReplication processorSnapshotReplicationConsumer;
  private SnapshotReplication exporterSnapshotReplicationConsumer;
  private StateStorage processorStateStorage;
  private StateStorage exporterStateStorage;
  private Supplier<Long> exporterPositionSupplier;
  private Supplier<Long> processorPositionSupplier;

  public void setProcessorSnapshotReplicationConsumer(
      SnapshotReplication processorSnapshotReplicationConsumer) {
    this.processorSnapshotReplicationConsumer = processorSnapshotReplicationConsumer;
  }

  public void setExporterSnapshotReplicationConsumer(
      SnapshotReplication exporterSnapshotReplicationConsumer) {
    this.exporterSnapshotReplicationConsumer = exporterSnapshotReplicationConsumer;
  }

  public void setProcessorStateStorage(StateStorage processorStateStorage) {
    this.processorStateStorage = processorStateStorage;
  }

  public void setExporterStateStorage(StateStorage exporterStateStorage) {
    this.exporterStateStorage = exporterStateStorage;
  }

  public void setExporterPositionSupplier(Supplier<Long> exporterPositionSupplier) {
    this.exporterPositionSupplier = exporterPositionSupplier;
  }

  public void setProcessorPositionSupplier(Supplier<Long> processorPositionSupplier) {
    this.processorPositionSupplier = processorPositionSupplier;
  }

  @Override
  public SnapshotReplication createProcessorSnapshotReplicationConsumer(int partitionId) {
    return processorSnapshotReplicationConsumer;
  }

  @Override
  public SnapshotReplication createExporterSnapshotReplicationConsumer(int partitionId) {
    return exporterSnapshotReplicationConsumer;
  }

  @Override
  public StateStorage getProcessorStateStorage(int partitionId) {
    return processorStateStorage;
  }

  @Override
  public StateStorage getExporterStateStorage(int partitionId) {
    return exporterStateStorage;
  }

  @Override
  public Supplier<Long> getExporterPositionSupplier(StateStorage exporterStorage) {
    return exporterPositionSupplier;
  }

  @Override
  public Supplier<Long> getProcessorPositionSupplier(
      int partitionId, StateStorage processorStorage) {
    return processorPositionSupplier;
  }
}
