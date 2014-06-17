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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.message.qosreport.QosReport;
import eu.stratosphere.nephele.streaming.taskmanager.StreamTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintViolationListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosModel;

/**
 * @author Bernd Louis (bernd.louis@gmail.com)
 */
@PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
		AllocatedResource.class })
@RunWith(PowerMockRunner.class)
public class QosModelTest {
	private QosGraphFixture fix;
	@Mock
	private QosConstraintViolationListener qosConstraintViolationListener;

	private JobInputVertex jobInputVertex;
	private JobTaskVertex jobTaskVertex;
	private JobGenericOutputVertex jobGenericOutputVertex;
	private JobGraph simpleJobGraph;

	@SuppressWarnings("unused")
	private StreamTaskManagerPlugin streamTaskManagerPlugin;

	@Before
	public void setUp() throws Exception {
		this.fix = new QosGraphFixture();
		this.streamTaskManagerPlugin = new StreamTaskManagerPlugin();
		this.initializeSimpleGraph();
	}

	@Test
	public void testConstruction() throws Exception {
		QosGraph qg = this.createQosGraph(this.fix.jobGraph,
				this.fix.constraint1);
		QosModel qm = new QosModel(this.fix.jobGraph.getJobID());
		qm.mergeShallowQosGraph(qg);
	}

	/**
	 * Trying to merge an empty
	 * 
	 * @throws Exception
	 */
	@Test
	public void testEmptyGraph() throws Exception {
		QosGraph qg = new QosGraph();
		QosModel qm = new QosModel(new JobID());
		qm.mergeShallowQosGraph(qg);
		assertFalse(qm.isEmpty());
		assertFalse(qm.isShallow());
		assertTrue(qm.isReady());
		QosReport qosReport = new QosReport();
		qm.processQosReport(qosReport);
	}

	/**
	 * TODO is this correct?
	 * 
	 * @throws Exception
	 */
	@Test
	public void testEmpty() throws Exception {
		QosModel model = new QosModel(this.simpleJobGraph.getJobID());
		assertTrue(model.isEmpty());
	}

	@Test
	public void testCreateShallowGraphAndModel() throws Exception {
		QosGraph qosShallowGraph = this.createQosGraph(this.fix.jobGraph,
				this.fix.constraint1).cloneWithoutMembers();
		assertTrue(qosShallowGraph.isShallow());
		QosModel model = new QosModel(this.fix.jobGraph.getJobID());
		model.mergeShallowQosGraph(qosShallowGraph);
		assertTrue(model.isShallow());
	}

	private QosGraph createQosGraph(JobGraph jg,
			JobGraphLatencyConstraint constraint)
			throws GraphConversionException {
		InstanceType instanceType = new InstanceType();
		InstanceManager instanceManager = PowerMockito
				.mock(InstanceManager.class);
		when(instanceManager.getDefaultInstanceType()).thenReturn(instanceType);
		ExecutionGraph eg = new ExecutionGraph(jg, instanceManager);
		return QosGraphFactory.createConstrainedQosGraph(eg, constraint);
	}

	private void initializeSimpleGraph() throws JobGraphDefinitionException {
		this.simpleJobGraph = new JobGraph();
		this.jobInputVertex = new JobInputVertex("v1", this.simpleJobGraph);
		this.jobInputVertex.setInputClass(QosGraphFixture.DummyInputTask.class);

		this.jobTaskVertex = new JobTaskVertex("v2", this.simpleJobGraph);
		this.jobTaskVertex.setTaskClass(QosGraphFixture.DummyTask23.class);

		this.jobGenericOutputVertex = new JobGenericOutputVertex("v3",
				this.simpleJobGraph);
		this.jobGenericOutputVertex
				.setOutputClass(QosGraphFixture.DummyOutputTask.class);

		this.jobInputVertex.connectTo(this.jobTaskVertex, ChannelType.NETWORK,
				DistributionPattern.POINTWISE);
		this.jobTaskVertex.connectTo(this.jobGenericOutputVertex,
				ChannelType.NETWORK,
				DistributionPattern.POINTWISE);
	}
}
