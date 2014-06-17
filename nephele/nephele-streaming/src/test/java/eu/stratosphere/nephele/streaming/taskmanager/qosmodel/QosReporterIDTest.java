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

import org.junit.Test;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class QosReporterIDTest {

	@Test
	public void testReportIDVertex() {
		ExecutionVertexID vertexID = new ExecutionVertexID();
		GateID inGate = new GateID();
		GateID outGate = new GateID();
		QosReporterID repID = QosReporterID
				.forVertex(vertexID, inGate, outGate);

		ExecutionVertexID otherID = new ExecutionVertexID();
		QosReporterID otherRepID = QosReporterID.forVertex(otherID,
				new GateID(), new GateID());

		assertFalse(repID.equals(otherRepID));

		otherID.setID(vertexID);
		assertFalse(repID.equals(otherRepID));

		assertTrue(repID.equals(QosReporterID.forVertex(vertexID, inGate,
				outGate)));
	}

	@Test
	public void testChannelIDVertex() {
		ChannelID channelID = new ChannelID();
		QosReporterID repID = QosReporterID.forEdge(channelID);

		ChannelID otherChannelID = new ChannelID();
		if (channelID.equals(otherChannelID)) {
			assertTrue(repID.equals(QosReporterID.forEdge(otherChannelID)));
		} else {
			assertFalse(repID.equals(QosReporterID.forEdge(otherChannelID)));
		}

		otherChannelID.setID(channelID);
		assertTrue(repID.equals(QosReporterID.forEdge(otherChannelID)));
	}

}
