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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;

/**
 * @author Bjoern Lohrmann
 * 
 */
@PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
		AllocatedResource.class })
@RunWith(PowerMockRunner.class)
public class QosGraphFactoryTest {

	private QosGraphFixture fix;

	@Before
	public void setup() throws Exception {
		this.fix = new QosGraphFixture();
	}

	@Test
	public void testCreateConstrainedQosGraphWithConstraint1() throws Exception {
		/**
		 * constraint1 covers e13,v3,e34,v4,e45
		 */
		QosGraph graph = QosGraphFactory.createConstrainedQosGraph(
				this.fix.execGraph, this.fix.constraint1);
		assertEquals(4, graph.getNumberOfVertices());

		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex1,
				graph);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex3,
				graph);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex4,
				graph);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex5,
				graph);

		assertEquals(1, graph.getStartVertices().size());
		assertEquals(this.fix.vertex1, graph.getStartVertices().iterator()
				.next());
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(this.fix.vertex5, graph.getEndVertices().iterator().next());
	}

	@Test
	public void testCreateConstrainedQosGraphWithConstraint2() throws Exception {
		/**
		 * constraint2 covers e12,v2,e24,v4
		 */
		QosGraph graph = QosGraphFactory.createConstrainedQosGraph(
				this.fix.execGraph, this.fix.constraint2);
		assertEquals(3, graph.getNumberOfVertices());

		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex1,
				graph);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex2,
				graph);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex4,
				graph);

		assertEquals(1, graph.getStartVertices().size());
		assertEquals(this.fix.vertex1, graph.getStartVertices().iterator()
				.next());
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(this.fix.vertex4, graph.getEndVertices().iterator().next());
	}

	@Test
	public void testCreateConstrainedQosGraphWithConstraint3() throws Exception {
		/**
		 * constraint3 covers v2,e24,v4,e45
		 */
		QosGraph graph = QosGraphFactory.createConstrainedQosGraph(
				this.fix.execGraph, this.fix.constraint3);
		assertEquals(3, graph.getNumberOfVertices());

		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex2,
				graph);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex4,
				graph);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex5,
				graph);

		assertEquals(1, graph.getStartVertices().size());
		assertEquals(this.fix.vertex2, graph.getStartVertices().iterator()
				.next());
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(this.fix.vertex5, graph.getEndVertices().iterator().next());
	}

	@Test
	public void testCreateConstrainedQosGraphWithConstraint4() throws Exception {
		/**
		 * constraint4 covers v2,e24,v4
		 */
		QosGraph graph = QosGraphFactory.createConstrainedQosGraph(
				this.fix.execGraph, this.fix.constraint4);
		assertEquals(2, graph.getNumberOfVertices());

		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex2,
				graph);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex4,
				graph);

		assertEquals(1, graph.getStartVertices().size());
		assertEquals(this.fix.vertex2, graph.getStartVertices().iterator()
				.next());
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(this.fix.vertex4, graph.getEndVertices().iterator().next());
	}

	@Test
	public void testCreateConstrainedSubgraphWithOneAnchorAtGraphStart() {
		/**
		 * constraint1 covers e13,v3,e34,v4,e45
		 */
		QosGraph graph = new QosGraph(this.fix.vertex10);
		graph.addConstraint(this.fix.constraint5);
		QosGraph subgraph = QosGraphFactory.createConstrainedSubgraph(graph,
				this.fix.constraint5.getID(),
				Collections.singletonList(this.fix.vertex10.getMember(0)));

		this.assertSubgraphGroupStructure(subgraph);

		// special case asserts about member counts
		// caused by subgraphing
		assertEquals(1,
				subgraph.getGroupVertexByID(this.fix.vertex10.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(this.fix.vertex10.getMember(0), subgraph
				.getGroupVertexByID(this.fix.vertex10.getJobVertexID())
				.getMember(0));
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex11.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex12.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex13.getJobVertexID())
						.getNumberOfMembers());
	}

	private void assertSubgraphGroupStructure(QosGraph subgraph) {
		// general asserts about subgraph's structure
		assertEquals(4, subgraph.getNumberOfVertices());
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex10,
				subgraph, false);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex11,
				subgraph, false);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex12,
				subgraph, false);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex13,
				subgraph, false);
		assertEquals(1, subgraph.getStartVertices().size());
		assertEquals(this.fix.vertex10, subgraph.getStartVertices().iterator()
				.next());
		assertEquals(1, subgraph.getEndVertices().size());
		assertEquals(this.fix.vertex13, subgraph.getEndVertices().iterator()
				.next());
	}

	@Test
	public void testCreateConstrainedSubgraphWithTwoAnchorsAtGraphStart() {
		/**
		 * constraint1 covers e13,v3,e34,v4,e45
		 */
		QosGraph graph = new QosGraph(this.fix.vertex10);
		graph.addConstraint(this.fix.constraint5);
		QosGraph subgraph = QosGraphFactory.createConstrainedSubgraph(
				graph,
				this.fix.constraint5.getID(),
				Arrays.asList(this.fix.vertex10.getMember(0),
						this.fix.vertex10.getMember(1)));

		this.assertSubgraphGroupStructure(subgraph);

		// special case asserts about member counts
		// caused by subgraphing
		assertEquals(2,
				subgraph.getGroupVertexByID(this.fix.vertex10.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(this.fix.vertex10.getMember(0), subgraph
				.getGroupVertexByID(this.fix.vertex10.getJobVertexID())
				.getMember(0));
		assertEquals(this.fix.vertex10.getMember(1), subgraph
				.getGroupVertexByID(this.fix.vertex10.getJobVertexID())
				.getMember(1));
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex11.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex12.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex13.getJobVertexID())
						.getNumberOfMembers());
	}

	@Test
	public void testCreateConstrainedSubgraphWithOneAnchorAtGraphCenter() {
		/**
		 * constraint1 covers e13,v3,e34,v4,e45
		 */
		QosGraph graph = new QosGraph(this.fix.vertex10);
		graph.addConstraint(this.fix.constraint5);
		QosGraph subgraph = QosGraphFactory.createConstrainedSubgraph(graph,
				this.fix.constraint5.getID(),
				Collections.singletonList(this.fix.vertex11.getMember(0)));

		this.assertSubgraphGroupStructure(subgraph);

		// special case asserts about member counts
		// caused by subgraphing
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex10.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(1,
				subgraph.getGroupVertexByID(this.fix.vertex11.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(this.fix.vertex11.getMember(0), subgraph
				.getGroupVertexByID(this.fix.vertex11.getJobVertexID())
				.getMember(0));
		assertEquals(1,
				subgraph.getGroupVertexByID(this.fix.vertex12.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(this.fix.vertex12.getMember(0), subgraph
				.getGroupVertexByID(this.fix.vertex12.getJobVertexID())
				.getMember(0));
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex13.getJobVertexID())
						.getNumberOfMembers());
	}

	@Test
	public void testCreateConstrainedSubgraphWithTwoAnchorsAtGraphCenter() {
		/**
		 * constraint1 covers e13,v3,e34,v4,e45
		 */
		QosGraph graph = new QosGraph(this.fix.vertex10);
		graph.addConstraint(this.fix.constraint5);
		QosGraph subgraph = QosGraphFactory.createConstrainedSubgraph(
				graph,
				this.fix.constraint5.getID(),
				Arrays.asList(this.fix.vertex12.getMember(1),
						this.fix.vertex12.getMember(2)));

		this.assertSubgraphGroupStructure(subgraph);

		// special case asserts about member counts
		// caused by subgraphing
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex10.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(2,
				subgraph.getGroupVertexByID(this.fix.vertex11.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(this.fix.vertex11.getMember(1), subgraph
				.getGroupVertexByID(this.fix.vertex11.getJobVertexID())
				.getMember(1));
		assertEquals(this.fix.vertex11.getMember(2), subgraph
				.getGroupVertexByID(this.fix.vertex11.getJobVertexID())
				.getMember(2));
		assertEquals(2,
				subgraph.getGroupVertexByID(this.fix.vertex12.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(this.fix.vertex12.getMember(1), subgraph
				.getGroupVertexByID(this.fix.vertex12.getJobVertexID())
				.getMember(1));
		assertEquals(this.fix.vertex12.getMember(2), subgraph
				.getGroupVertexByID(this.fix.vertex12.getJobVertexID())
				.getMember(2));
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex13.getJobVertexID())
						.getNumberOfMembers());
	}

	@Test
	public void testCreateConstrainedSubgraphWithOneAnchorAtGraphEnd() {
		/**
		 * constraint1 covers e13,v3,e34,v4,e45
		 */
		QosGraph graph = new QosGraph(this.fix.vertex10);
		graph.addConstraint(this.fix.constraint5);
		QosGraph subgraph = QosGraphFactory.createConstrainedSubgraph(graph,
				this.fix.constraint5.getID(),
				Collections.singletonList(this.fix.vertex13.getMember(1)));

		this.assertSubgraphGroupStructure(subgraph);

		// special case asserts about member counts
		// caused by subgraphing
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex10.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex11.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex12.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(1,
				subgraph.getGroupVertexByID(this.fix.vertex13.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(this.fix.vertex13.getMember(1), subgraph
				.getGroupVertexByID(this.fix.vertex13.getJobVertexID())
				.getMember(1));
	}

	@Test
	public void testCreateConstrainedSubgraphWithTwoAnchorsAtGraphEnd() {
		/**
		 * constraint1 covers e13,v3,e34,v4,e45
		 */
		QosGraph graph = new QosGraph(this.fix.vertex10);
		graph.addConstraint(this.fix.constraint5);
		QosGraph subgraph = QosGraphFactory.createConstrainedSubgraph(
				graph,
				this.fix.constraint5.getID(),
				Arrays.asList(this.fix.vertex13.getMember(1),
						this.fix.vertex13.getMember(2)));

		this.assertSubgraphGroupStructure(subgraph);

		// special case asserts about member counts
		// caused by subgraphing
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex10.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex11.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(3,
				subgraph.getGroupVertexByID(this.fix.vertex12.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(2,
				subgraph.getGroupVertexByID(this.fix.vertex13.getJobVertexID())
						.getNumberOfMembers());
		assertEquals(this.fix.vertex13.getMember(1), subgraph
				.getGroupVertexByID(this.fix.vertex13.getJobVertexID())
				.getMember(1));

		assertEquals(this.fix.vertex13.getMember(2), subgraph
				.getGroupVertexByID(this.fix.vertex13.getJobVertexID())
				.getMember(2));
	}

}
