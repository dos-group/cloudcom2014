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
package eu.stratosphere.nephele.streaming.message.action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * Describes a Qos reporter role for an edge (channel).
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class EdgeQosReporterConfig implements IOReadableWritable {

	private ChannelID sourceChannelID;

	private ChannelID targetChannelID;

	private InstanceConnectionInfo[] qosManagers;

	private GateID outputGateID;

	private GateID inputGateID;

	private int outputGateEdgeIndex;

	private int inputGateEdgeIndex;

	private String name;

	public EdgeQosReporterConfig() {
	}

	/**
	 * Initializes EdgeQosReporterConfig.
	 * 
	 * @param sourceChannelID
	 * @param targetChannelID
	 * @param qosManagers
	 * @param action
	 * @param outputGateIndex
	 * @param inputGateIndex
	 * @param outputGateEdgeIndex
	 * @param inputGateEdgeIndex
	 */
	public EdgeQosReporterConfig(ChannelID sourceChannelID,
			ChannelID targetChannelID, InstanceConnectionInfo[] qosManagers,
			GateID outputGateID, GateID inputGateID, int outputGateEdgeIndex,
			int inputGateEdgeIndex, String name) {

		this.sourceChannelID = sourceChannelID;
		this.targetChannelID = targetChannelID;
		this.qosManagers = qosManagers;
		this.outputGateID = outputGateID;
		this.inputGateID = inputGateID;
		this.outputGateEdgeIndex = outputGateEdgeIndex;
		this.inputGateEdgeIndex = inputGateEdgeIndex;
		this.name = name;
	}

	/**
	 * Returns the sourceChannelID.
	 * 
	 * @return the sourceChannelID
	 */
	public ChannelID getSourceChannelID() {
		return this.sourceChannelID;
	}

	/**
	 * Returns the targetChannelID.
	 * 
	 * @return the targetChannelID
	 */
	public ChannelID getTargetChannelID() {
		return this.targetChannelID;
	}

	/**
	 * Returns the qosManagers.
	 * 
	 * @return the qosManagers
	 */
	public InstanceConnectionInfo[] getQosManagers() {
		return this.qosManagers;
	}

	/**
	 * Returns the outputGateID.
	 * 
	 * @return the outputGateID
	 */
	public GateID getOutputGateID() {
		return this.outputGateID;
	}

	/**
	 * Returns the inputGateID.
	 * 
	 * @return the inputGateID
	 */
	public GateID getInputGateID() {
		return this.inputGateID;
	}

	/**
	 * Returns the outputGateEdgeIndex.
	 * 
	 * @return the outputGateEdgeIndex
	 */
	public int getOutputGateEdgeIndex() {
		return this.outputGateEdgeIndex;
	}

	/**
	 * Returns the inputGateEdgeIndex.
	 * 
	 * @return the inputGateEdgeIndex
	 */
	public int getInputGateEdgeIndex() {
		return this.inputGateEdgeIndex;
	}

	/**
	 * Returns the name.
	 * 
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	public QosEdge toQosEdge() {
		return new QosEdge(this.sourceChannelID, this.targetChannelID,
				this.outputGateEdgeIndex, this.inputGateEdgeIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.sourceChannelID.write(out);
		this.targetChannelID.write(out);
		out.writeInt(this.qosManagers.length);
		for (InstanceConnectionInfo qosManager : this.qosManagers) {
			qosManager.write(out);
		}
		this.outputGateID.write(out);
		this.inputGateID.write(out);
		out.writeInt(this.outputGateEdgeIndex);
		out.writeInt(this.inputGateEdgeIndex);
		out.writeUTF(this.name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.sourceChannelID = new ChannelID();
		this.sourceChannelID.read(in);
		this.targetChannelID = new ChannelID();
		this.targetChannelID.read(in);
		this.qosManagers = new InstanceConnectionInfo[in.readInt()];
		for (int i = 0; i < this.qosManagers.length; i++) {
			this.qosManagers[i] = new InstanceConnectionInfo();
			this.qosManagers[i].read(in);
		}
		this.outputGateID = new GateID();
		this.outputGateID.read(in);
		this.inputGateID = new GateID();
		this.inputGateID.read(in);
		this.outputGateEdgeIndex = in.readInt();
		this.inputGateEdgeIndex = in.readInt();
		this.name = in.readUTF();
	}

	public QosReporterID getReporterID() {
		return QosReporterID.forEdge(this.sourceChannelID);
	}
}
