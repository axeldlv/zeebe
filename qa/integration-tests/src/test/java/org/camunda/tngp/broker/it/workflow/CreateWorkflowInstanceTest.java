package org.camunda.tngp.broker.it.workflow;

import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.tngp.broker.it.ClientRule;
import org.camunda.tngp.broker.it.EmbeddedBrokerRule;
import org.camunda.tngp.client.TngpClient;
import org.camunda.tngp.client.WorkflowTopicClient;
import org.camunda.tngp.client.workflow.cmd.WorkflowInstance;
import org.camunda.tngp.client.workflow.cmd.WorkflowInstanceRejectedException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import static org.assertj.core.api.Assertions.assertThat;

public class CreateWorkflowInstanceTest
{

    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();

    public ClientRule clientRule = new ClientRule();

    @Rule
    public RuleChain ruleChain = RuleChain
        .outerRule(brokerRule)
        .around(clientRule);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void deployProcess()
    {
        final TngpClient client = clientRule.getClient();
        final WorkflowTopicClient workflowService = client.workflowTopic(0);

        workflowService.deploy()
            .bpmnModelInstance(
                Bpmn.createExecutableProcess("anId")
                    .startEvent()
                    .endEvent()
                    .done())
            .execute();

        workflowService.deploy()
            .bpmnModelInstance(
                Bpmn.createExecutableProcess("anId")
                    .startEvent()
                    .endEvent()
                    .done())
            .execute();
    }

    @Test
    public void shouldCreateBpmnProcessById()
    {
        final TngpClient client = clientRule.getClient();
        final WorkflowTopicClient workflowService = client.workflowTopic(0);

        // when
        final WorkflowInstance workflowInstance =
            workflowService
                .create()
                .bpmnProcessId("anId")
                .execute();

        // then instance of latest of workflow version is created
        assertThat(workflowInstance.getBpmnProcessId()).isEqualTo("anId");
        assertThat(workflowInstance.getVersion()).isEqualTo(2);
        assertThat(workflowInstance.getWorkflowInstanceKey()).isGreaterThan(0);
    }


    @Test
    public void shouldCreateBpmnProcessByIdAndVersion()
    {
        final TngpClient client = clientRule.getClient();
        final WorkflowTopicClient workflowService = client.workflowTopic(0);


        // when
        final WorkflowInstance workflowInstance =
            workflowService
                .create()
                .bpmnProcessId("anId")
                .version(1)
                .execute();

        // then instance is created of first workflow version
        assertThat(workflowInstance.getBpmnProcessId()).isEqualTo("anId");
        assertThat(workflowInstance.getVersion()).isEqualTo(1);
        assertThat(workflowInstance.getWorkflowInstanceKey()).isGreaterThan(0);
    }

    @Test
    public void shouldRejectCreateBpmnProcessByIllegalId()
    {
        final TngpClient client = clientRule.getClient();
        final WorkflowTopicClient workflowService = client.workflowTopic(0);

        // expected
        exception.expect(WorkflowInstanceRejectedException.class);
        exception.expectMessage("Creation of workflow instance with id illegal and version -1 was rejected.");

        // when
        workflowService
            .create()
            .bpmnProcessId("illegal")
            .execute();
    }

    @Test
    public void shouldThrowExceptionForCreateBpmnProcessByNullBpmnProcessId()
    {
        final TngpClient client = clientRule.getClient();
        final WorkflowTopicClient workflowService = client.workflowTopic(0);

        // expected
        exception.expect(RuntimeException.class);
        exception.expectMessage("bpmnProcessId must not be null");

        // when
        workflowService
            .create()
            .bpmnProcessId(null)
            .execute();
    }

    @Test
    public void shouldThrowExceptionForCreateBpmnProcessByEmptyBpmnProcessId()
    {
        final TngpClient client = clientRule.getClient();
        final WorkflowTopicClient workflowService = client.workflowTopic(0);

        // expected
        exception.expect(RuntimeException.class);
        exception.expectMessage("bpmnProcessId must not be empty");

        // when
        workflowService
            .create()
            .bpmnProcessId("")
            .execute();
    }

    @Test
    public void shouldThrowExceptionForCreateBpmnProcessByIllegalVersion()
    {
        final TngpClient client = clientRule.getClient();
        final WorkflowTopicClient workflowService = client.workflowTopic(0);

        // expected
        exception.expect(RuntimeException.class);
        exception.expectMessage("version must be greater than or equal to -1");

        // when
        workflowService
            .create()
            .bpmnProcessId("anId")
            .version(-10)
            .execute();
    }
}