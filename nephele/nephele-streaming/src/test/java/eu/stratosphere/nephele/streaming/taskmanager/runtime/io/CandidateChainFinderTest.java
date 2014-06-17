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
package eu.stratosphere.nephele.streaming.taskmanager.runtime.io;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.jobmanager.CandidateChainFinder;
import eu.stratosphere.nephele.streaming.jobmanager.CandidateChainListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFactory;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFixture;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFixture2;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFixture3;

/**
 * Tests for
 * {@link eu.stratosphere.nephele.streaming.jobmanager.CandidateChainFinder}
 * 
 */
@PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
		AllocatedResource.class })
@RunWith(PowerMockRunner.class)
public class CandidateChainFinderTest {
	private QosGraphFixture fix;
	private QosGraphFixture2 fix2;
	private QosGraphFixture3 fix3;

	@Mock
	private CandidateChainListener listener;

	@Before
	public void setUp() throws Exception {
		this.fix = new QosGraphFixture();
		this.fix2 = new QosGraphFixture2();
		this.fix3 = new QosGraphFixture3();
	}

	/**
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testPositiveCon4() throws Exception {

		QosGraph constrainedQosGraph = QosGraphFactory
				.createConstrainedQosGraph(this.fix.execGraph,
						this.fix.constraint4);
		CandidateChainFinder f = new CandidateChainFinder(this.listener,
				this.fix.execGraph);
		f.findChainsAlongConstraint(this.fix.constraint4.getID(),
				constrainedQosGraph);
		verify(this.listener, never()).handleCandidateChain(
				any(InstanceConnectionInfo.class), any(LinkedList.class));
	}

	/**
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testPositiveCon3() throws Exception {
		QosGraph constrainedQosGraph = QosGraphFactory
				.createConstrainedQosGraph(this.fix.execGraph,
						this.fix.constraint3);
		CandidateChainFinder f = new CandidateChainFinder(this.listener,
				this.fix.execGraph);
		f.findChainsAlongConstraint(this.fix.constraint3.getID(),
				constrainedQosGraph);
		verify(this.listener, never()).handleCandidateChain(
				any(InstanceConnectionInfo.class), any(LinkedList.class));
	}

	/**
	 * -> V -> should not cause a chain.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testEdgeVertexEdge() throws Exception {
		QosGraph constrainedQosGraph = QosGraphFactory
				.createConstrainedQosGraph(this.fix2.executionGraph,
						this.fix2.constraintEdgeVertexEdge);
		CandidateChainFinder f = new CandidateChainFinder(this.listener,
				this.fix2.executionGraph);
		f.findChainsAlongConstraint(this.fix2.constraintEdgeVertexEdge.getID(),
				constrainedQosGraph);
		verify(this.listener, never()).handleCandidateChain(
				any(InstanceConnectionInfo.class), any(LinkedList.class));
	}

	/**
	 * Tests a subgraph t1 -> t2 ->
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testVertexEdgeVertexEdge() throws Exception {
		QosGraph constrainedQosGraph = QosGraphFactory
				.createConstrainedQosGraph(this.fix2.executionGraph,
						this.fix2.constraintVertexEdgeVertexEdge);
		CandidateChainFinder f = new CandidateChainFinder(this.listener,
				this.fix2.executionGraph);
		f.findChainsAlongConstraint(
				this.fix2.constraintVertexEdgeVertexEdge.getID(),
				constrainedQosGraph);
		verify(this.listener, times(2)).handleCandidateChain(
				any(InstanceConnectionInfo.class), any(LinkedList.class));
	}

	/**
	 * Tests a sequence of the full graph described in {@link QosGraphFixture2}.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testFullSequence() throws Exception {
		QosGraph constrainedQosGraph = QosGraphFactory
				.createConstrainedQosGraph(this.fix2.executionGraph,
						this.fix2.constraintFull);
		CandidateChainFinder f = new CandidateChainFinder(this.listener,
				this.fix2.executionGraph);
		f.findChainsAlongConstraint(this.fix2.constraintFull.getID(),
				constrainedQosGraph);
		verify(this.listener, atLeastOnce()).handleCandidateChain(
				any(InstanceConnectionInfo.class), any(LinkedList.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMiddle() throws Exception {
		QosGraph constrainedQosGraph = QosGraphFactory
				.createConstrainedQosGraph(this.fix2.executionGraph,
						this.fix2.constraintMiddleOfGraph);
		new CandidateChainFinder(this.listener, this.fix2.executionGraph)
				.findChainsAlongConstraint(
						this.fix2.constraintMiddleOfGraph.getID(),
						constrainedQosGraph);
		verify(this.listener, times(2)).handleCandidateChain(
				any(InstanceConnectionInfo.class), any(LinkedList.class));

	}

	/**
	 * There should not be a chain from input to task1 because the edge between
	 * is bipartite.
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testBipartite() {
		QosGraph constrainedQosGraph = QosGraphFactory
				.createConstrainedQosGraph(this.fix2.executionGraph,
						this.fix2.constraintInputVertexEdgeVertex);
		new CandidateChainFinder(this.listener, this.fix2.executionGraph)
				.findChainsAlongConstraint(
						this.fix2.constraintInputVertexEdgeVertex.getID(),
						constrainedQosGraph);
		verify(this.listener, never()).handleCandidateChain(
				any(InstanceConnectionInfo.class), any(LinkedList.class));
	}

    /**
     * Checks if the {@link QosGraphFixture3#jobGraph} produces
     * 2 calls with disjoint {@link ExecutionVertexID} lists.
     *
     * @throws Exception
     */
	@SuppressWarnings("unchecked")
	@Test
	public void testFix3Handling() throws Exception {
		QosGraph graphLeft = QosGraphFactory.createConstrainedQosGraph(
				this.fix3.executionGraph, this.fix3.constraintFull);

		final LinkedList<ExecutionVertexID> chain1 = new LinkedList<ExecutionVertexID>();
		final LinkedList<ExecutionVertexID> chain2 = new LinkedList<ExecutionVertexID>();
		CandidateChainListener chainListener = new CandidateChainListener() {
			@Override
			public void handleCandidateChain(
					InstanceConnectionInfo executingInstance,
					LinkedList<ExecutionVertexID> chain) {
				// we're looking for 2 chains here, excess calls will appear on
				// the spy (see below)
				if (chain1.isEmpty())
					chain1.addAll(chain);
				else if (chain2.isEmpty())
					chain2.addAll(chain);
			}
		};
		CandidateChainListener listenerSpy = spy(chainListener);

		new CandidateChainFinder(listenerSpy, this.fix3.executionGraph)
				.findChainsAlongConstraint(this.fix3.constraintFull.getID(),
						graphLeft);

		// check for the correct number of calls
		verify(listenerSpy, times(2)).handleCandidateChain(
				any(InstanceConnectionInfo.class), any(LinkedList.class));

		// check that lists are disjoint
		for (ExecutionVertexID v : chain1)
			if (chain2.contains(v))
				fail(String.format(
						"Reported chains should be disjoint but they aren't (%s is in both lists)", v));
	}
}
