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

import org.apache.commons.lang3.builder.HashCodeBuilder;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * This class stores information about the latency of a vertex (task).
 * 
 * @author warneke, Bjoern Lohrmann
 */
public final class VertexLatency extends AbstractQosReportRecord {

	private QosReporterID.Vertex reporterID;

	private int counter;

	private double vertexLatency;

	/**
	 * Constructs a new task latency object.
	 * 
	 * @param jobID
	 *            the ID of the job this path latency information refers to
	 * @param vertexID
	 *            the ID of the vertex this task latency information refers to
	 * @param taskLatency
	 *            the task latency in milliseconds
	 */
	public VertexLatency(final QosReporterID.Vertex reporterID,
			final double vertexLatency) {

		this.reporterID = reporterID;
		this.vertexLatency = vertexLatency;
		this.counter = 1;
	}

	/**
	 * Default constructor for the deserialization of the object.
	 */
	public VertexLatency() {
	}

	public QosReporterID.Vertex getReporterID() {
		return this.reporterID;
	}

	/**
	 * Returns the task latency in milliseconds.
	 * 
	 * @return the task latency in milliseconds
	 */
	public double getVertexLatency() {
		return this.vertexLatency / this.counter;
	}

	public void add(VertexLatency taskLatency) {
		this.counter++;
		this.vertexLatency += taskLatency.getVertexLatency();
	}

	@Override
	public boolean equals(Object otherObj) {
		boolean isEqual = false;
		if (otherObj instanceof VertexLatency) {
			VertexLatency other = (VertexLatency) otherObj;
			isEqual = other.getReporterID().equals(this.getReporterID())
					&& other.getVertexLatency() == this.getVertexLatency();
		}

		return isEqual;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(this.vertexLatency)
				.append(this.reporterID).toHashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.reporterID.write(out);
		out.writeDouble(this.getVertexLatency());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.reporterID = new QosReporterID.Vertex();
		this.reporterID.read(in);
		this.vertexLatency = in.readDouble();
	}
}
