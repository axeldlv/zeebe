/*
 * Zeebe Workflow Engine
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
package io.zeebe.engine.processor;

import static io.zeebe.protocol.intent.WorkflowInstanceIntent.ELEMENT_ACTIVATED;
import static io.zeebe.protocol.intent.WorkflowInstanceIntent.ELEMENT_ACTIVATING;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import io.zeebe.engine.util.StreamProcessorRule;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.verification.VerificationWithTimeout;

public class StreamProcessorReprocessingTest {

  private static final long TIMEOUT_MILLIS = 2_000L;
  private static final VerificationWithTimeout TIMEOUT = timeout(TIMEOUT_MILLIS);

  @Rule public StreamProcessorRule streamProcessorRule = new StreamProcessorRule();

  @Test
  public void shouldCallRecordProcessorLifecycle() {
    // given
    final long position = streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATING, 1);
    final long secondPosition =
        streamProcessorRule.writeWorkflowInstanceEventWithSource(
            WorkflowInstanceIntent.ELEMENT_ACTIVATED, 1, position);
    waitUntil(
        () ->
            streamProcessorRule
                .events()
                .onlyWorkflowInstanceRecords()
                .withIntent(ELEMENT_ACTIVATED)
                .exists());

    // when
    final TypedRecordProcessor typedRecordProcessor = mock(TypedRecordProcessor.class);
    final StreamProcessor streamProcessor =
        streamProcessorRule.startTypedStreamProcessor(
            (processors, state) ->
                processors
                    .onEvent(ValueType.WORKFLOW_INSTANCE, ELEMENT_ACTIVATING, typedRecordProcessor)
                    .onEvent(ValueType.WORKFLOW_INSTANCE, ELEMENT_ACTIVATED, typedRecordProcessor));

    verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(secondPosition), any(), any(), any(), any());
    streamProcessor.closeAsync().join();

    // then
    final InOrder inOrder = inOrder(typedRecordProcessor);
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onOpen(any());
    // reprocessing
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(position), any(), any(), any(), any());
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onRecovered(any());
    // normal processing
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(secondPosition), any(), any(), any(), any());
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onClose();

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldReprocessUntilLastSource() {
    // given
    final long firstEvent = streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATING, 1);
    final long secondEvent = streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATING, 1);
    final long lastSourceEvent =
        streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATING, 1);
    final long normalProcessingPosition =
        streamProcessorRule.writeWorkflowInstanceEventWithSource(
            WorkflowInstanceIntent.ELEMENT_ACTIVATED, 1, lastSourceEvent);
    waitUntil(
        () ->
            streamProcessorRule
                .events()
                .onlyWorkflowInstanceRecords()
                .withIntent(ELEMENT_ACTIVATED)
                .exists());

    // when
    final TypedRecordProcessor typedRecordProcessor = mock(TypedRecordProcessor.class);
    final StreamProcessor streamProcessor =
        streamProcessorRule.startTypedStreamProcessor(
            (processors, state) ->
                processors
                    .onEvent(ValueType.WORKFLOW_INSTANCE, ELEMENT_ACTIVATING, typedRecordProcessor)
                    .onEvent(ValueType.WORKFLOW_INSTANCE, ELEMENT_ACTIVATED, typedRecordProcessor));

    verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(normalProcessingPosition), any(), any(), any(), any());
    streamProcessor.closeAsync().join();

    // then
    final InOrder inOrder = inOrder(typedRecordProcessor);
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onOpen(any());
    // reprocessing
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(firstEvent), any(), any(), any(), any());
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(secondEvent), any(), any(), any(), any());
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(lastSourceEvent), any(), any(), any(), any());
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onRecovered(any());
    // normal processing
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(normalProcessingPosition), any(), any(), any(), any());
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onClose();

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldNotReprocessWithoutSourcePosition() {
    // given
    final long position = streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATING, 1);
    final long secondPosition =
        streamProcessorRule.writeWorkflowInstanceEvent(WorkflowInstanceIntent.ELEMENT_ACTIVATED, 1);
    waitUntil(
        () ->
            streamProcessorRule
                .events()
                .onlyWorkflowInstanceRecords()
                .withIntent(ELEMENT_ACTIVATED)
                .exists());

    // when
    final TypedRecordProcessor typedRecordProcessor = mock(TypedRecordProcessor.class);
    final StreamProcessor streamProcessor =
        streamProcessorRule.startTypedStreamProcessor(
            (processors, state) ->
                processors
                    .onEvent(ValueType.WORKFLOW_INSTANCE, ELEMENT_ACTIVATING, typedRecordProcessor)
                    .onEvent(ValueType.WORKFLOW_INSTANCE, ELEMENT_ACTIVATED, typedRecordProcessor));

    verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(secondPosition), any(), any(), any(), any());
    streamProcessor.closeAsync().join();

    // then
    final InOrder inOrder = inOrder(typedRecordProcessor);
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onOpen(any());
    // no reprocessing
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onRecovered(any());
    // normal processing
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(position), any(), any(), any(), any());
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(secondPosition), any(), any(), any(), any());
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onClose();

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldNotReprocessIfNotSameProducer() {
    // given
    final long position =
        streamProcessorRule.writeWorkflowInstanceEventWithDifferentProducerId(
            ELEMENT_ACTIVATING, 1, 255);
    final long secondPosition =
        streamProcessorRule.writeWorkflowInstanceEventWithDifferentProducerIdAndSource(
            WorkflowInstanceIntent.ELEMENT_ACTIVATED, 1, 255, position);
    waitUntil(
        () ->
            streamProcessorRule
                .events()
                .onlyWorkflowInstanceRecords()
                .withIntent(ELEMENT_ACTIVATED)
                .exists());

    // when
    final TypedRecordProcessor typedRecordProcessor = mock(TypedRecordProcessor.class);
    final StreamProcessor streamProcessor =
        streamProcessorRule.startTypedStreamProcessor(
            (processors, state) ->
                processors
                    .onEvent(ValueType.WORKFLOW_INSTANCE, ELEMENT_ACTIVATING, typedRecordProcessor)
                    .onEvent(ValueType.WORKFLOW_INSTANCE, ELEMENT_ACTIVATED, typedRecordProcessor));

    verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(secondPosition), any(), any(), any(), any());
    streamProcessor.closeAsync().join();

    // then
    final InOrder inOrder = inOrder(typedRecordProcessor);
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onOpen(any());
    // no reprocessing
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onRecovered(any());
    // normal processing
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(position), any(), any(), any(), any());
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(secondPosition), any(), any(), any(), any());
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(2)).onClose();

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldRetryProcessingRecordOnException() {
    // given
    final long firstPosition =
        streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATING, 1);
    streamProcessorRule.writeWorkflowInstanceEventWithSource(ELEMENT_ACTIVATED, 1, firstPosition);

    waitUntil(
        () ->
            streamProcessorRule
                .events()
                .onlyWorkflowInstanceRecords()
                .withIntent(ELEMENT_ACTIVATED)
                .exists());

    // when
    final TypedRecordProcessor<?> typedRecordProcessor = mock(TypedRecordProcessor.class);
    final AtomicInteger count = new AtomicInteger(0);
    doAnswer(
            (invocationOnMock -> {
              if (count.getAndIncrement() == 0) {
                throw new RuntimeException("recoverable");
              }
              return null;
            }))
        .when(typedRecordProcessor)
        .processRecord(anyLong(), any(), any(), any(), any());
    streamProcessorRule.startTypedStreamProcessor(
        (processors, state) ->
            processors.onEvent(
                ValueType.WORKFLOW_INSTANCE, ELEMENT_ACTIVATING, typedRecordProcessor));

    // then
    final InOrder inOrder = inOrder(typedRecordProcessor);
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(1)).onOpen(any());
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(2))
        .processRecord(eq(firstPosition), any(), any(), any(), any());
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(1)).onRecovered(any());

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldIgnoreRecordWhenNoProcessorExistForThisType() {
    // given
    final long firstPosition = streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATED, 1);
    final long secondPosition =
        streamProcessorRule.writeWorkflowInstanceEventWithSource(
            WorkflowInstanceIntent.ELEMENT_ACTIVATING, 1, firstPosition);

    // when
    final TypedRecordProcessor<?> typedRecordProcessor = mock(TypedRecordProcessor.class);
    streamProcessorRule.startTypedStreamProcessor(
        (processors, state) ->
            processors.onEvent(
                ValueType.WORKFLOW_INSTANCE, ELEMENT_ACTIVATING, typedRecordProcessor));

    // then
    final InOrder inOrder = inOrder(typedRecordProcessor);
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(1)).onOpen(any());
    inOrder
        .verify(typedRecordProcessor, never())
        .processRecord(eq(firstPosition), any(), any(), any(), any());
    inOrder.verify(typedRecordProcessor, TIMEOUT.times(1)).onRecovered(any());
    inOrder
        .verify(typedRecordProcessor, TIMEOUT.times(1))
        .processRecord(eq(secondPosition), any(), any(), any(), any());

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldNotWriteFollowUpEvent() throws Exception {
    // given
    final long firstPosition =
        streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATING, 1);
    streamProcessorRule.writeWorkflowInstanceEventWithSource(ELEMENT_ACTIVATED, 1, firstPosition);

    waitUntil(
        () ->
            streamProcessorRule
                .events()
                .onlyWorkflowInstanceRecords()
                .withIntent(ELEMENT_ACTIVATED)
                .exists());

    // when
    final CountDownLatch processLatch = new CountDownLatch(1);
    streamProcessorRule.startTypedStreamProcessor(
        (processors, state) ->
            processors
                .onEvent(
                    ValueType.WORKFLOW_INSTANCE,
                    ELEMENT_ACTIVATING,
                    new TypedRecordProcessor<UnpackedObject>() {
                      @Override
                      public void processRecord(
                          long position,
                          TypedRecord<UnpackedObject> record,
                          TypedResponseWriter responseWriter,
                          TypedStreamWriter streamWriter,
                          Consumer<SideEffectProducer> sideEffect) {
                        streamWriter.appendFollowUpEvent(
                            record.getKey(),
                            WorkflowInstanceIntent.ELEMENT_ACTIVATED,
                            record.getValue());
                      }
                    })
                .onEvent(
                    ValueType.WORKFLOW_INSTANCE,
                    ELEMENT_ACTIVATED,
                    new TypedRecordProcessor<UnpackedObject>() {
                      @Override
                      public void processRecord(
                          long position,
                          TypedRecord<UnpackedObject> record,
                          TypedResponseWriter responseWriter,
                          TypedStreamWriter streamWriter,
                          Consumer<SideEffectProducer> sideEffect) {
                        processLatch.countDown();
                      }
                    }));

    // then
    processLatch.await();

    final long eventCount =
        streamProcessorRule
            .events()
            .onlyWorkflowInstanceRecords()
            .withIntent(ELEMENT_ACTIVATED)
            .count();
    assertThat(eventCount).isEqualTo(1);
  }

  @Test
  public void shouldStartFromLastSnapshotPosition() throws Exception {
    // given
    final CountDownLatch processingLatch = new CountDownLatch(2);
    streamProcessorRule.startTypedStreamProcessor(
        (processors, state) ->
            processors.onEvent(
                ValueType.WORKFLOW_INSTANCE,
                ELEMENT_ACTIVATING,
                new TypedRecordProcessor<UnpackedObject>() {
                  @Override
                  public void processRecord(
                      long position,
                      TypedRecord<UnpackedObject> record,
                      TypedResponseWriter responseWriter,
                      TypedStreamWriter streamWriter,
                      Consumer<SideEffectProducer> sideEffect) {
                    processingLatch.countDown();
                  }
                }));

    streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATING);
    streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATING);
    processingLatch.await();
    streamProcessorRule.closeStreamProcessor();

    // when
    final List<Long> processedPositions = new ArrayList<>();
    final CountDownLatch newProcessLatch = new CountDownLatch(1);
    streamProcessorRule.startTypedStreamProcessor(
        (processors, state) ->
            processors.onEvent(
                ValueType.WORKFLOW_INSTANCE,
                ELEMENT_ACTIVATING,
                new TypedRecordProcessor<UnpackedObject>() {
                  @Override
                  public void processRecord(
                      long position,
                      TypedRecord<UnpackedObject> record,
                      TypedResponseWriter responseWriter,
                      TypedStreamWriter streamWriter,
                      Consumer<SideEffectProducer> sideEffect) {
                    processedPositions.add(position);
                    newProcessLatch.countDown();
                  }
                }));
    final long position = streamProcessorRule.writeWorkflowInstanceEvent(ELEMENT_ACTIVATING);

    // then
    newProcessLatch.await();

    assertThat(processedPositions).containsExactly(position);
  }
}
