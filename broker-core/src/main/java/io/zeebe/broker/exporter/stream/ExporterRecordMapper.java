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
package io.zeebe.broker.exporter.stream;

import io.zeebe.broker.exporter.ExporterObjectMapper;
import io.zeebe.broker.exporter.record.RecordImpl;
import io.zeebe.broker.exporter.record.value.*;
import io.zeebe.broker.exporter.record.value.deployment.DeployedWorkflowImpl;
import io.zeebe.broker.exporter.record.value.deployment.DeploymentResourceImpl;
import io.zeebe.broker.exporter.record.value.job.HeadersImpl;
import io.zeebe.exporter.api.record.Record;
import io.zeebe.exporter.api.record.RecordMetadata;
import io.zeebe.exporter.api.record.RecordValue;
import io.zeebe.exporter.api.record.value.*;
import io.zeebe.exporter.api.record.value.deployment.DeployedWorkflow;
import io.zeebe.exporter.api.record.value.deployment.DeploymentResource;
import io.zeebe.exporter.api.record.value.deployment.ResourceType;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.msgpack.value.LongValue;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.deployment.Workflow;
import io.zeebe.protocol.impl.record.value.error.ErrorRecord;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.zeebe.protocol.impl.record.value.job.JobBatchRecord;
import io.zeebe.protocol.impl.record.value.job.JobHeaders;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.impl.record.value.message.MessageRecord;
import io.zeebe.protocol.impl.record.value.message.MessageStartEventSubscriptionRecord;
import io.zeebe.protocol.impl.record.value.message.MessageSubscriptionRecord;
import io.zeebe.protocol.impl.record.value.message.WorkflowInstanceSubscriptionRecord;
import io.zeebe.protocol.impl.record.value.timer.TimerRecord;
import io.zeebe.protocol.impl.record.value.variable.VariableDocumentRecord;
import io.zeebe.protocol.impl.record.value.variable.VariableRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceCreationRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.util.buffer.BufferUtil;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.agrona.DirectBuffer;
import org.agrona.io.DirectBufferInputStream;

public class ExporterRecordMapper {
  private final DirectBufferInputStream serderInputStream = new DirectBufferInputStream();
  private final ExporterObjectMapper objectMapper;

  public ExporterRecordMapper(final ExporterObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public Record map(final LoggedEvent event, final RecordMetadata metadata) {
    final Function<DirectBuffer, ? extends RecordValue> valueSupplier;

    switch (metadata.getValueType()) {
      case DEPLOYMENT:
        valueSupplier = this::ofDeploymentRecord;
        break;
      case INCIDENT:
        valueSupplier = this::ofIncidentRecord;
        break;
      case JOB:
        valueSupplier = this::ofJobRecord;
        break;
      case MESSAGE:
        valueSupplier = this::ofMessageRecord;
        break;
      case MESSAGE_SUBSCRIPTION:
        valueSupplier = this::ofMessageSubscriptionRecord;
        break;
      case WORKFLOW_INSTANCE:
        valueSupplier = this::ofWorkflowInstanceRecord;
        break;
      case WORKFLOW_INSTANCE_SUBSCRIPTION:
        valueSupplier = this::ofWorkflowInstanceSubscriptionRecord;
        break;
      case JOB_BATCH:
        valueSupplier = this::ofJobBatchRecord;
        break;
      case TIMER:
        valueSupplier = this::ofTimerRecord;
        break;
      case MESSAGE_START_EVENT_SUBSCRIPTION:
        valueSupplier = this::ofMessageStartEventSubscriptionRecord;
        break;
      case VARIABLE:
        valueSupplier = this::ofVariableRecord;
        break;
      case VARIABLE_DOCUMENT:
        valueSupplier = this::ofVariableDocumentRecord;
        break;
      case WORKFLOW_INSTANCE_CREATION:
        valueSupplier = this::ofWorkflowInstanceCreationRecord;
        break;
      case ERROR:
        valueSupplier = this::ofErrorRecord;
        break;
      default:
        return null;
    }

    return newRecord(event, metadata, valueSupplier);
  }

  private <T extends RecordValue> RecordImpl<T> newRecord(
      final LoggedEvent event,
      final RecordMetadata metadata,
      final Function<DirectBuffer, T> valueSupplier) {

    // need to copy the event buffer for lazy transformation
    final DirectBuffer eventBuffer =
        BufferUtil.cloneBuffer(
            event.getValueBuffer(), event.getValueOffset(), event.getValueLength());

    return new RecordImpl<>(
        objectMapper,
        event.getKey(),
        event.getPosition(),
        Instant.ofEpochMilli(event.getTimestamp()),
        event.getProducerId(),
        event.getSourceEventPosition(),
        metadata,
        () -> valueSupplier.apply(eventBuffer));
  }

  private JobRecordValue ofJobRecord(final DirectBuffer valueBuffer) {
    final JobRecord record = new JobRecord();
    record.wrap(valueBuffer);

    return ofJobRecord(record);
  }

  private JobRecordValue ofJobRecord(JobRecord record) {
    final JobHeaders jobHeaders = record.getHeaders();
    final HeadersImpl headers =
        new HeadersImpl(
            asString(jobHeaders.getBpmnProcessId()),
            asString(jobHeaders.getElementId()),
            jobHeaders.getElementInstanceKey(),
            jobHeaders.getWorkflowInstanceKey(),
            jobHeaders.getWorkflowKey(),
            jobHeaders.getWorkflowDefinitionVersion());

    final Instant deadline;
    if (record.getDeadline() != Protocol.INSTANT_NULL_VALUE) {
      deadline = Instant.ofEpochMilli(record.getDeadline());
    } else {
      deadline = null;
    }

    return new JobRecordValueImpl(
        objectMapper,
        asJson(record.getVariables()),
        asMsgPackMap(record.getVariables()),
        asString(record.getType()),
        asString(record.getWorker()),
        deadline,
        headers,
        asMsgPackMap(record.getCustomHeaders()),
        record.getRetries(),
        asString(record.getErrorMessage()));
  }

  private DeploymentRecordValue ofDeploymentRecord(final DirectBuffer valueBuffer) {
    final List<DeployedWorkflow> deployedWorkflows = new ArrayList<>();
    final List<DeploymentResource> resources = new ArrayList<>();
    final DeploymentRecord record = new DeploymentRecord();

    record.wrap(valueBuffer);

    for (final Workflow workflow : record.workflows()) {
      deployedWorkflows.add(
          new DeployedWorkflowImpl(
              asString(workflow.getBpmnProcessId()),
              asString(workflow.getResourceName()),
              workflow.getKey(),
              workflow.getVersion()));
    }

    for (final io.zeebe.protocol.impl.record.value.deployment.DeploymentResource resource :
        record.resources()) {
      resources.add(
          new DeploymentResourceImpl(
              asByteArray(resource.getResource()),
              asResourceType(resource.getResourceType()),
              asString(resource.getResourceName())));
    }

    return new io.zeebe.broker.exporter.record.value.DeploymentRecordValueImpl(
        objectMapper, deployedWorkflows, resources);
  }

  private IncidentRecordValue ofIncidentRecord(final DirectBuffer valueBuffer) {
    final IncidentRecord record = new IncidentRecord();
    record.wrap(valueBuffer);

    return new IncidentRecordValueImpl(
        objectMapper,
        record.getErrorType().name(),
        asString(record.getErrorMessage()),
        asString(record.getBpmnProcessId()),
        asString(record.getElementId()),
        record.getWorkflowKey(),
        record.getWorkflowInstanceKey(),
        record.getElementInstanceKey(),
        record.getJobKey(),
        record.getVariableScopeKey());
  }

  private MessageRecordValue ofMessageRecord(final DirectBuffer valueBuffer) {
    final MessageRecord record = new MessageRecord();
    record.wrap(valueBuffer);

    return new io.zeebe.broker.exporter.record.value.MessageRecordValueImpl(
        objectMapper,
        asJson(record.getVariables()),
        asMsgPackMap(record.getVariables()),
        asString(record.getName()),
        asString(record.getMessageId()),
        asString(record.getCorrelationKey()),
        record.getTimeToLive());
  }

  private MessageSubscriptionRecordValue ofMessageSubscriptionRecord(
      final DirectBuffer valueBuffer) {
    final MessageSubscriptionRecord record = new MessageSubscriptionRecord();
    record.wrap(valueBuffer);

    return new MessageSubscriptionRecordValueImpl(
        objectMapper,
        asString(record.getMessageName()),
        asString(record.getCorrelationKey()),
        record.getWorkflowInstanceKey(),
        record.getElementInstanceKey());
  }

  private MessageStartEventSubscriptionRecordValueImpl ofMessageStartEventSubscriptionRecord(
      final DirectBuffer valueBuffer) {
    final MessageStartEventSubscriptionRecord record = new MessageStartEventSubscriptionRecord();
    record.wrap(valueBuffer);

    return new MessageStartEventSubscriptionRecordValueImpl(
        objectMapper,
        record.getWorkflowKey(),
        asString(record.getStartEventId()),
        asString(record.getMessageName()));
  }

  private WorkflowInstanceRecordValue ofWorkflowInstanceRecord(final DirectBuffer valueBuffer) {
    final WorkflowInstanceRecord record = new WorkflowInstanceRecord();
    record.wrap(valueBuffer);

    return new WorkflowInstanceRecordValueImpl(
        objectMapper,
        asString(record.getBpmnProcessId()),
        asString(record.getElementId()),
        record.getVersion(),
        record.getWorkflowKey(),
        record.getWorkflowInstanceKey(),
        record.getFlowScopeKey(),
        record.getBpmnElementType());
  }

  private WorkflowInstanceSubscriptionRecordValue ofWorkflowInstanceSubscriptionRecord(
      final DirectBuffer valueBuffer) {
    final WorkflowInstanceSubscriptionRecord record = new WorkflowInstanceSubscriptionRecord();
    record.wrap(valueBuffer);

    return new WorkflowInstanceSubscriptionRecordValueImpl(
        objectMapper,
        asJson(record.getVariables()),
        asMsgPackMap(record.getVariables()),
        asString(record.getMessageName()),
        record.getWorkflowInstanceKey(),
        record.getElementInstanceKey());
  }

  private RecordValue ofJobBatchRecord(DirectBuffer valueBuffer) {
    final JobBatchRecord record = new JobBatchRecord();
    record.wrap(valueBuffer);

    final List<Long> jobKeys =
        StreamSupport.stream(record.jobKeys().spliterator(), false)
            .map(LongValue::getValue)
            .collect(Collectors.toList());

    final List<JobRecordValue> jobs =
        StreamSupport.stream(record.jobs().spliterator(), false)
            .map(this::ofJobRecord)
            .collect(Collectors.toList());

    return new JobBatchRecordValueImpl(
        objectMapper,
        asString(record.getType()),
        asString(record.getWorker()),
        Duration.ofMillis(record.getTimeout()),
        record.getMaxJobsToActivate(),
        jobKeys,
        jobs,
        record.getTruncated());
  }

  private RecordValue ofTimerRecord(DirectBuffer valueBuffer) {
    final TimerRecord record = new TimerRecord();
    record.wrap(valueBuffer);

    return new TimerRecordValueImpl(
        objectMapper,
        record.getElementInstanceKey(),
        record.getWorkflowInstanceKey(),
        record.getDueDate(),
        asString(record.getHandlerNodeId()),
        record.getRepetitions(),
        record.getWorkflowKey());
  }

  private VariableRecordValue ofVariableRecord(DirectBuffer valueBuffer) {
    final VariableRecord record = new VariableRecord();
    record.wrap(valueBuffer);

    return new VariableRecordValueImpl(
        objectMapper,
        asString(record.getName()),
        asJson(record.getValue()),
        record.getScopeKey(),
        record.getWorkflowInstanceKey(),
        record.getWorkflowKey());
  }

  private VariableDocumentRecordValue ofVariableDocumentRecord(DirectBuffer valueBuffer) {
    final VariableDocumentRecord record = new VariableDocumentRecord();
    record.wrap(valueBuffer);

    return new VariableDocumentRecordValueImpl(
        objectMapper,
        record.getScopeKey(),
        record.getUpdateSemantics(),
        asMsgPackMap(record.getDocument()));
  }

  private WorkflowInstanceCreationRecordValue ofWorkflowInstanceCreationRecord(
      DirectBuffer valueBuffer) {
    final WorkflowInstanceCreationRecord record = new WorkflowInstanceCreationRecord();
    record.wrap(valueBuffer);

    return new WorkflowInstanceCreationRecordValueImpl(
        objectMapper,
        asString(record.getBpmnProcessId()),
        record.getVersion(),
        record.getKey(),
        record.getInstanceKey(),
        asMsgPackMap(record.getVariables()));
  }

  private ErrorRecordValue ofErrorRecord(final DirectBuffer valueBuffer) {
    final ErrorRecord record = new ErrorRecord();
    record.wrap(valueBuffer);

    return new ErrorRecordValueImpl(
        objectMapper,
        asString(record.getExceptionMessage()),
        asString(record.getStacktrace()),
        record.getErrorEventPosition(),
        record.getWorkflowInstanceKey());
  }

  private byte[] asByteArray(final DirectBuffer buffer) {
    return BufferUtil.bufferAsArray(buffer);
  }

  private String asString(final DirectBuffer buffer) {
    return BufferUtil.bufferAsString(buffer);
  }

  private Supplier<Map<String, Object>> asMsgPackMap(final DirectBuffer msgPackEncoded) {
    return () -> {
      serderInputStream.wrap(msgPackEncoded);
      return objectMapper.fromMsgpackAsMap(serderInputStream);
    };
  }

  private Supplier<String> asJson(final DirectBuffer msgPackEncoded) {
    return () -> {
      serderInputStream.wrap(msgPackEncoded);
      return objectMapper.getMsgPackConverter().convertToJson(serderInputStream);
    };
  }

  private ResourceType asResourceType(
      final io.zeebe.protocol.impl.record.value.deployment.ResourceType resourceType) {
    switch (resourceType) {
      case BPMN_XML:
        return ResourceType.BPMN_XML;
      case YAML_WORKFLOW:
        return ResourceType.YAML_WORKFLOW;
    }
    throw new IllegalArgumentException("Provided resource type does not exist " + resourceType);
  }
}
