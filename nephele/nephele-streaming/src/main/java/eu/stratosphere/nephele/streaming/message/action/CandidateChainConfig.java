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
import java.util.LinkedList;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * Describes a candidate chain of vertices to the task manager. A candidate
 * chain is a list of {@link ExecutionVertexID} objects, that fulfill the
 * following conditions: (1) Each vertex on the chain must have exactly one
 * input and one output gate, except the first, which may have multiple input
 * gates and the last, which may have multiple output gates. (2) The output gate
 * of the first vertex in the chain must connect to the input gate of the
 * second, the second to the third, and so on. (3) The output/input gates inside
 * the chain must have exactly one channel (in JobGraph terms, they must use the
 * {@link eu.stratosphere.nephele.io.DistributionPattern#POINTWISE} distributed
 * pattern).
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class CandidateChainConfig implements IOReadableWritable {

	public final LinkedList<ExecutionVertexID> chainingCandidates;

	public CandidateChainConfig() {
		this.chainingCandidates = new LinkedList<ExecutionVertexID>();
	}

	public CandidateChainConfig(LinkedList<ExecutionVertexID> chainingCandidates) {
		this.chainingCandidates = chainingCandidates;
	}

	/**
	 * Returns the chainingCandidates.
	 * 
	 * @return the chainingCandidates
	 */
	public LinkedList<ExecutionVertexID> getChainingCandidates() {
		return this.chainingCandidates;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.chainingCandidates.size());
		for (ExecutionVertexID vertexID : this.chainingCandidates) {
			vertexID.write(out);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		int noOfVertices = in.readInt();
		for (int i = 0; i < noOfVertices; i++) {
			ExecutionVertexID vertexID = new ExecutionVertexID();
			vertexID.read(in);
			this.chainingCandidates.add(vertexID);
		}
	}
}
