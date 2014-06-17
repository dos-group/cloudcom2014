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
 * This class holds statistical information about an edge (output channel side),
 * such as throughput, output buffer lifetime, records per buffer and records
 * per second.
 * 
 * @author Bjoern Lohrmann
 */
public final class EdgeStatistics extends AbstractQosReportRecord {

	/**
	 * The ID of reporter.
	 */
	private QosReporterID.Edge reporterID;

	/**
	 * The throughput in MBit/s.
	 */
	private double throughput;

	/**
	 * The lifetime of an output buffer on this specific output channel in
	 * millis.
	 */
	private double outputBufferLifetime;

	/**
	 * The number of records that fit into an output buffer of this channel.
	 */
	private double recordsPerBuffer;

	/**
	 * The number of records per second the task emits on the channel.
	 */
	private double recordsPerSecond;

	/**
	 * Instances of this class may be used for aggregating other instances (see
	 * {@see #add(ChannelThroughput)}). This counter keeps track of how many
	 * other instances have been aggregated.
	 */
	private int counter;

	/**
	 * Default constructor for deserialization.
	 */
	public EdgeStatistics() {
	}

	/**
	 * /** Constructs a new channel throughput object.
	 * 
	 * @param reporterID
	 *            the ID of the QOs reporter
	 * @param throughput
	 *            throughput of the output channel in MBit/s
	 * @param outputBufferLifetime
	 *            lifetime of an output buffer on this specific output channel
	 *            in millis
	 * @param recordsPerBuffer
	 *            number of records per output buffer on this channel
	 * @param recordsPerSecond
	 *            number of records that are emitted on this channel each second
	 */
	public EdgeStatistics(QosReporterID.Edge reporterID, double throughput,
			double outputBufferLifetime, double recordsPerBuffer,
			double recordsPerSecond) {

		this.reporterID = reporterID;
		this.throughput = throughput;
		this.outputBufferLifetime = outputBufferLifetime;
		this.recordsPerBuffer = recordsPerBuffer;
		this.recordsPerSecond = recordsPerSecond;
		this.counter = 1;

	}

	/**
	 * Returns the throughput of the output channel in MBit/s.
	 * 
	 * @return the throughput of the output channel in MBit/s.
	 */
	public double getThroughput() {
		return this.throughput / this.counter;
	}

	/**
	 * Returns the lifetime of an output buffer on this specific output channel
	 * in millis.
	 * 
	 * @return the lifetime of an output buffer on this specific output channel
	 *         in millis.
	 */
	public double getOutputBufferLifetime() {
		return this.outputBufferLifetime / this.counter;
	}

	/**
	 * Returns the number of records that fit into an output buffer of this
	 * channel.
	 * 
	 * @return the number of records that fit into an output buffer of this
	 *         channel.
	 */
	public double getRecordsPerBuffer() {
		return this.recordsPerBuffer / this.counter;
	}

	/**
	 * Returns the number of records per second the task emits on the channel.
	 * 
	 * @return the number of records per second the task emits on the channel.
	 */
	public double getRecordsPerSecond() {
		return this.recordsPerSecond / this.counter;
	}

	/**
	 * Returns the reporterID.
	 * 
	 * @return the reporterID
	 */
	public QosReporterID.Edge getReporterID() {
		return this.reporterID;
	}

	public void add(EdgeStatistics channelThroughput) {
		this.throughput += channelThroughput.throughput;
		this.outputBufferLifetime += channelThroughput.outputBufferLifetime;
		this.recordsPerBuffer += channelThroughput.recordsPerBuffer;
		this.recordsPerSecond += channelThroughput.recordsPerSecond;
		this.counter += channelThroughput.counter;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.reporterID.write(out);
		out.writeDouble(this.getThroughput());
		out.writeDouble(this.getOutputBufferLifetime());
		out.writeDouble(this.getRecordsPerBuffer());
		out.writeDouble(this.getRecordsPerSecond());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.reporterID = new QosReporterID.Edge();
		this.reporterID.read(in);
		this.throughput = in.readDouble();
		this.outputBufferLifetime = in.readDouble();
		this.recordsPerBuffer = in.readDouble();
		this.recordsPerSecond = in.readDouble();
		this.counter = 1;
	}

	@Override
	public boolean equals(Object otherObj) {
		boolean isEqual = false;
		if (otherObj instanceof EdgeStatistics) {
			EdgeStatistics other = (EdgeStatistics) otherObj;
			isEqual = other.reporterID.equals(this.reporterID)
					&& other.getThroughput() == this.getThroughput()
					&& other.getOutputBufferLifetime() == this
							.getOutputBufferLifetime()
					&& other.getRecordsPerBuffer() == this
							.getRecordsPerBuffer()
					&& other.getRecordsPerSecond() == this
							.getRecordsPerSecond();
		}

		return isEqual;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return this.reporterID.hashCode();
	}
}
