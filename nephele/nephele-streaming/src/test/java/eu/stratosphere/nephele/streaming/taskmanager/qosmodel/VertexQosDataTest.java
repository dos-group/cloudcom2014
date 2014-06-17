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

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;

/**
 * Tests on {@link VertexQosData} class.
 * 
 */
@PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
		AllocatedResource.class })
@RunWith(PowerMockRunner.class)
public class VertexQosDataTest {
	private QosGraphFixture fix;

	@Before
	public void setUp() throws Exception {
		this.fix = new QosGraphFixture();
	}

	/**
	 * This test forces the VertexQosData to resize its internal
	 * {@link VertexQosData#qosStatistics} field to make room for the specified
	 * gate combination's statistical data.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPrepareForReportsOnGateCombination() throws Exception {
		InstanceManager instanceManager = mock(InstanceManager.class);
		when(instanceManager.getDefaultInstanceType()).thenReturn(
				new InstanceType());
		ExecutionGraph eg = new ExecutionGraph(this.fix.jobGraph,
				instanceManager);
		QosGraph constrainedQosGraph = QosGraphFactory
				.createConstrainedQosGraph(eg, this.fix.constraint1);
		QosGroupVertex someQosVertex = constrainedQosGraph.getAllVertices()
				.iterator().next();
		VertexQosData vertexQosData = new VertexQosData(
				someQosVertex.getMember(0));
		vertexQosData.prepareForReportsOnGateCombination(100, 100);
	}
}
