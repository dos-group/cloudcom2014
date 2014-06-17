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
package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.plugins.PluginManager;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeQosData;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphMember;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.VertexQosData;

/**
 * This class is used by Qos managers to log aggregated Qos report data for a
 * given Qos constraint.
 * 
 * @author Bjoern Lohrmann
 */
public class QosLogger {
	/**
	 * Provides access to the configuration entry which defines the log file
	 * location.
	 */
	private static final String LOGFILE_PATTERN_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.logging.qos_statistics_filepattern");

	private static final String DEFAULT_LOGFILE_PATTERN = "/tmp/qos_statistics_%s";

	private BufferedWriter writer;

	private double[][] aggregatedMemberLatencies;

	private int[][] inputOutputGateCombinations;

	private double minTotalLatency;

	private double aggregatedTotalLatency;

	private double maxTotalLatency;

	private int activeMemberSequences;

	private long loggingInterval;

	public QosLogger(LatencyConstraintID constraintID, QosGraph qosGraph,
			long loggingInterval) throws IOException {

		JobGraphSequence jobGraphSequence = qosGraph.getConstraintByID(
				constraintID).getSequence();

		this.aggregatedMemberLatencies = new double[jobGraphSequence.size()][];
		this.inputOutputGateCombinations = new int[jobGraphSequence.size()][];
		for (SequenceElement<JobVertexID> sequenceElement : jobGraphSequence) {
			int index = sequenceElement.getIndexInSequence();

			this.inputOutputGateCombinations[index] = new int[2];
			if (sequenceElement.isVertex()) {
				this.aggregatedMemberLatencies[index] = new double[1];
				this.inputOutputGateCombinations[index][0] = sequenceElement
						.getInputGateIndex();
				this.inputOutputGateCombinations[index][1] = sequenceElement
						.getOutputGateIndex();
			} else {
				this.aggregatedMemberLatencies[index] = new double[2];
				this.inputOutputGateCombinations[index][0] = sequenceElement
						.getOutputGateIndex();
				this.inputOutputGateCombinations[index][1] = sequenceElement
						.getInputGateIndex();
			}
		}
		this.resetCounters();

		this.loggingInterval = loggingInterval;

		String logFile = GlobalConfiguration.getString(LOGFILE_PATTERN_KEY, DEFAULT_LOGFILE_PATTERN);
		if (logFile.contains("%s")) {
			logFile = String.format(logFile, constraintID.toString());
		}
		this.writer = new BufferedWriter(new FileWriter(logFile));
		this.writeHeaders(jobGraphSequence, qosGraph);
	}

	private void resetCounters() {
		this.activeMemberSequences = 0;
		this.aggregatedTotalLatency = 0;
		this.minTotalLatency = Double.MAX_VALUE;
		this.maxTotalLatency = Double.MIN_VALUE;

		for (int i = 0; i < this.aggregatedMemberLatencies.length; i++) {
			for (int j = 0; j < this.aggregatedMemberLatencies[i].length; j++) {
				this.aggregatedMemberLatencies[i][j] = 0;
			}
		}
	}

	public void addMemberSequenceToLog(List<QosGraphMember> sequenceMembers) {
		double sequenceLatency = 0;
		int index = 0;

		for (QosGraphMember member : sequenceMembers) {
			if (member.isVertex()) {
				VertexQosData vertexQos = ((QosVertex) member).getQosData();

				int inputGateIndex = this.inputOutputGateCombinations[index][0];
				int outputGateIndex = this.inputOutputGateCombinations[index][1];

				double vertexLatency = vertexQos.getLatencyInMillis(
						inputGateIndex, outputGateIndex);
				this.aggregatedMemberLatencies[index][0] += vertexLatency;
				sequenceLatency += vertexLatency;
			} else {
				EdgeQosData edgeQos = ((QosEdge) member).getQosData();
				double outputBufferLatency = edgeQos
						.getOutputBufferLifetimeInMillis() / 2;
				this.aggregatedMemberLatencies[index][0] += outputBufferLatency;
				this.aggregatedMemberLatencies[index][1] += Math.max(0,
						edgeQos.getChannelLatencyInMillis()
								- outputBufferLatency);
				sequenceLatency += edgeQos.getChannelLatencyInMillis();
			}

			index++;
		}

		this.aggregatedTotalLatency += sequenceLatency;

		if (sequenceLatency < this.minTotalLatency) {
			this.minTotalLatency = sequenceLatency;
		}

		if (sequenceLatency > this.maxTotalLatency) {
			this.maxTotalLatency = sequenceLatency;
		}

		this.activeMemberSequences++;
	}

	public void logLatencies() throws IOException {

		StringBuilder builder = new StringBuilder();
		builder.append(this.getLogTimestamp());
		builder.append(';');
		builder.append(this.activeMemberSequences);
		builder.append(';');

		if (this.activeMemberSequences == 0) {
			this.appendDummyLine(builder);
		} else {
			this.appendSummaryLine(builder);
		}

		builder.append('\n');
		this.writer.write(builder.toString());
		this.writer.flush();

		this.resetCounters();
	}

	private void appendSummaryLine(StringBuilder builder) {
		builder.append(this.formatDouble(this.aggregatedTotalLatency
				/ this.activeMemberSequences));
		builder.append(';');
		builder.append(this.formatDouble(this.minTotalLatency));
		builder.append(';');
		builder.append(this.formatDouble(this.maxTotalLatency));

		for (int i = 0; i < this.aggregatedMemberLatencies.length; i++) {
			for (int j = 0; j < this.aggregatedMemberLatencies[i].length; j++) {
				builder.append(';');
				builder.append(this
						.formatDouble(this.aggregatedMemberLatencies[i][j]
								/ this.activeMemberSequences));
			}
		}
	}

	private void appendDummyLine(StringBuilder builder) {
		builder.append(this.formatDouble(0));
		builder.append(';');
		builder.append(this.formatDouble(0));
		builder.append(';');
		builder.append(this.formatDouble(0));

		for (int i = 0; i < this.aggregatedMemberLatencies.length; i++) {
			for (int j = 0; j < this.aggregatedMemberLatencies[i].length; j++) {
				builder.append(';');
				builder.append(this.formatDouble(0));
			}
		}
	}

	private String formatDouble(double doubleValue) {
		return String.format("%.2f", doubleValue);
	}

	private Object getLogTimestamp() {
		return QosUtils.alignToInterval(System.currentTimeMillis(),
				this.loggingInterval) / 1000;
	}

	private void writeHeaders(JobGraphSequence jobGraphSequence,
			QosGraph qosGraph) throws IOException {

		StringBuilder builder = new StringBuilder();
		builder.append("timestamp;");
		builder.append("noOfActivePaths;");
		builder.append("avgTotalPathLatency;");
		builder.append("minPathLatency;");
		builder.append("maxPathLatency");

		int edgeIndex = 0;

		for (SequenceElement<JobVertexID> sequenceElement : jobGraphSequence) {
			if (sequenceElement.isVertex()) {
				builder.append(';');
				builder.append(qosGraph.getGroupVertexByID(
						sequenceElement.getVertexID()).getName());
			} else {
				builder.append(';');
				builder.append("edge" + edgeIndex + "obl");
				builder.append(';');
				builder.append("edge" + edgeIndex);
				edgeIndex++;
			}
		}
		builder.append('\n');
		this.writer.write(builder.toString());
	}

}
