/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mockito.Matchers;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.streaming.ConstraintUtil;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;

/**
 * This graph fixture represents the graph
 * 
 * O(1)--BIP-->O(2)--P-->O(2)-BIP-->O(1)
 * 
 * @PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
 *                   AllocatedResource.class })
 * @RunWith(PowerMockRunner.class)
 * 
 */
public class QosGraphFixture3 {
	public JobGraph jobGraph;
	public JobInputVertex jobInputVertex;
	public JobTaskVertex jobTaskVertex1;
	public JobTaskVertex jobTaskVertex2;
	public JobOutputVertex jobOutputVertex;

	public ExecutionGraph executionGraph;
	public ExecutionGroupVertex executionInputVertex;
	public ExecutionGroupVertex executionTaskVertex1;
	public ExecutionGroupVertex executionTaskVertex2;
	public ExecutionGroupVertex executionOutputVertex;
	private InstanceConnectionInfo[][] instanceConnectionInfos;

	public JobGraphLatencyConstraint constraintFull;

	public QosGraphFixture3() throws JobGraphDefinitionException,
			GraphConversionException, IOException {
		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
			BasicConfigurator.configure();
			Logger.getRootLogger().setLevel(Level.WARN);
		}

		this.createJobGraph();
		this.createExecutionGraph();
		this.createConstraints();
	}

	private void createJobGraph() throws JobGraphDefinitionException {
		this.jobGraph = new JobGraph();

		this.jobInputVertex = new JobInputVertex("vInput", this.jobGraph);
		this.jobInputVertex
				.setInputClass(QosGraphTestUtil.DummyInputTask.class);
		this.jobInputVertex.setNumberOfSubtasks(1);

		this.jobTaskVertex1 = new JobTaskVertex("vTask1", this.jobGraph);
		this.jobTaskVertex1.setTaskClass(QosGraphTestUtil.DummyTask.class);
		this.jobTaskVertex1.setNumberOfSubtasks(2);

		this.jobTaskVertex2 = new JobTaskVertex("vTask2", this.jobGraph);
		this.jobTaskVertex2.setTaskClass(QosGraphTestUtil.DummyTask.class);
		this.jobTaskVertex2.setNumberOfSubtasks(2);

		this.jobOutputVertex = new JobOutputVertex("vOutput", this.jobGraph);
		this.jobOutputVertex
				.setOutputClass(QosGraphTestUtil.DummyOutputTask.class);
		this.jobOutputVertex.setNumberOfSubtasks(1);

		this.jobInputVertex.connectTo(this.jobTaskVertex1, ChannelType.NETWORK,
				DistributionPattern.BIPARTITE);
		this.jobTaskVertex1.connectTo(this.jobTaskVertex2, ChannelType.NETWORK,
				DistributionPattern.POINTWISE);
		this.jobTaskVertex2.connectTo(this.jobOutputVertex,
				ChannelType.NETWORK,
				DistributionPattern.BIPARTITE);
	}

	private void createExecutionGraph() throws GraphConversionException {
		mockStatic(ExecutionSignature.class);
		ExecutionSignature execSig = mock(ExecutionSignature.class);

		when(
				ExecutionSignature.createSignature(
						Matchers.eq(AbstractGenericInputTask.class),
						Matchers.any(JobID.class))).thenReturn(execSig);

		when(
				ExecutionSignature.createSignature(
						Matchers.eq(AbstractTask.class),
						Matchers.any(JobID.class))).thenReturn(execSig);

		when(
				ExecutionSignature.createSignature(
						Matchers.eq(AbstractOutputTask.class),
						Matchers.any(JobID.class))).thenReturn(execSig);

		InstanceManager instanceManager = mock(InstanceManager.class);
		InstanceType instanceType = new InstanceType();
		when(instanceManager.getDefaultInstanceType()).thenReturn(instanceType);
		this.executionGraph = new ExecutionGraph(this.jobGraph, instanceManager);

		ExecutionStage executionStage = this.executionGraph.getStage(0);
		for (int i = 0; i < executionStage.getNumberOfStageMembers(); i++) {
			ExecutionGroupVertex v = executionStage.getStageMember(i);
			if (v.getJobVertexID().equals(this.jobInputVertex.getID()))
				this.executionInputVertex = v;
			else if (v.getJobVertexID().equals(this.jobTaskVertex1.getID()))
				this.executionTaskVertex1 = v;
			else if (v.getJobVertexID().equals(this.jobTaskVertex2.getID()))
				this.executionTaskVertex2 = v;
			else if (v.getJobVertexID().equals(this.jobOutputVertex.getID()))
				this.executionOutputVertex = v;
		}
		this.instanceConnectionInfos = new InstanceConnectionInfo[4][];
		this.instanceConnectionInfos[0] = QosGraphTestUtil
				.generateAndAssignInstances(this.executionInputVertex);
		this.instanceConnectionInfos[1] = QosGraphTestUtil
				.generateAndAssignInstances(this.executionTaskVertex1);
		this.instanceConnectionInfos[2] = QosGraphTestUtil
				.generateAndAssignInstances(this.executionTaskVertex2);
		this.instanceConnectionInfos[3] = QosGraphTestUtil
				.generateAndAssignInstances(this.executionOutputVertex);
	}

	private void createConstraints() throws IOException {
		ConstraintUtil.defineAllLatencyConstraintsBetween(this.jobInputVertex,
				this.jobOutputVertex, 2000l);
		this.constraintFull = ConstraintUtil.getConstraints(
				this.jobGraph.getJobConfiguration()).get(0);
	}
}
