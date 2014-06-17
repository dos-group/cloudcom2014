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
package eu.stratosphere.nephele.streaming.message.qosreport;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * Indicates that a dummy Qos reporter (a vertex Qos reporter where either input
 * gate or output gate is not set) has become active. The purpose of this
 * message is to trigger a Qos reporter announcement in the QosReportForwarder
 * to be sent to the Qos manager. Using the information in the announcement the
 * Qos manager can complete its Qos graph model.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class DummyVertexReporterActivity extends AbstractQosReportRecord {

	private QosReporterID.Vertex reporterID;

	public DummyVertexReporterActivity(QosReporterID.Vertex reporterID) {
		if (!reporterID.isDummy()) {
			throw new RuntimeException(
					"DummyVertexReporterActivity messages are only supported for dummy reporters. This is a bug.");
		}
		this.reporterID = reporterID;
	}

	public DummyVertexReporterActivity() {
	}

	public QosReporterID.Vertex getReporterID() {
		return this.reporterID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.reporterID.write(out);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.reporterID = new QosReporterID.Vertex();
		this.reporterID.read(in);
	}
}
