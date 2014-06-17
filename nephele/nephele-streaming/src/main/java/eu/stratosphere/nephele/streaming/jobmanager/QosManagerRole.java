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
package eu.stratosphere.nephele.streaming.jobmanager;

import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

/**
 * Models a Qos manager role while computing the Qos setup on the job manager
 * side. A Qos manager role is defined by a set of "anchor" Qos vertices within
 * the same QosGroupVertex (this is the "anchor" group vertex). The anchor Qos
 * vertices must be executed on the same task manager. A Qos manager role is
 * always associated with exactly one constraint: QosManagerRole (1..N) <--> (1)
 * Constraint.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosManagerRole {

	private LatencyConstraintID constraintID;

	private QosGraph qosGraph;

	private QosGroupVertex anchorVertex;

	private List<QosVertex> membersOnInstance;

	/**
	 * Initializes ManagerRole.
	 * 
	 * @param qosGraph
	 * @param constraintID
	 * @param anchorVertex
	 */
	public QosManagerRole(QosGraph qosGraph, LatencyConstraintID constraintID,
			QosGroupVertex anchorGroupVertex, List<QosVertex> anchorVertices) {

		this.qosGraph = qosGraph;
		this.constraintID = constraintID;
		this.anchorVertex = anchorGroupVertex;
		this.membersOnInstance = anchorVertices;
	}

	public InstanceConnectionInfo getManagerInstance() {
		return this.membersOnInstance.get(0).getExecutingInstance();
	}

	/**
	 * Returns the constraintID.
	 * 
	 * @return the constraintID
	 */
	public LatencyConstraintID getConstraintID() {
		return this.constraintID;
	}

	/**
	 * Sets the constraintID to the specified value.
	 * 
	 * @param constraintID
	 *            the constraintID to set
	 */
	public void setConstraintID(LatencyConstraintID constraintID) {
		if (constraintID == null) {
			throw new NullPointerException("constraintID must not be null");
		}

		this.constraintID = constraintID;
	}

	/**
	 * Returns the qosGraph.
	 * 
	 * @return the qosGraph
	 */
	public QosGraph getQosGraph() {
		return this.qosGraph;
	}

	/**
	 * Sets the qosGraph to the specified value.
	 * 
	 * @param qosGraph
	 *            the qosGraph to set
	 */
	public void setQosGraph(QosGraph qosGraph) {
		if (qosGraph == null) {
			throw new NullPointerException("qosGraph must not be null");
		}

		this.qosGraph = qosGraph;
	}

	/**
	 * Returns the anchorVertex.
	 * 
	 * @return the anchorVertex
	 */
	public QosGroupVertex getAnchorVertex() {
		return this.anchorVertex;
	}

	/**
	 * Sets the anchorVertex to the specified value.
	 * 
	 * @param anchorVertex
	 *            the anchorVertex to set
	 */
	public void setAnchorVertex(QosGroupVertex anchorVertex) {
		if (anchorVertex == null) {
			throw new NullPointerException("anchorVertex must not be null");
		}

		this.anchorVertex = anchorVertex;
	}

	/**
	 * Returns the membersOnInstance.
	 * 
	 * @return the membersOnInstance
	 */
	public List<QosVertex> getMembersOnInstance() {
		return this.membersOnInstance;
	}

	/**
	 * Sets the membersOnInstance to the specified value.
	 * 
	 * @param membersOnInstance
	 *            the membersOnInstance to set
	 */
	public void setMembersOnInstance(List<QosVertex> membersOnInstance) {
		if (membersOnInstance == null) {
			throw new NullPointerException("membersOnInstance must not be null");
		}

		this.membersOnInstance = membersOnInstance;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(this.constraintID)
				.append(this.membersOnInstance.get(0).getID()).toHashCode();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj.getClass() != this.getClass()) {
			return false;
		}
		QosManagerRole rhs = (QosManagerRole) obj;
		return new EqualsBuilder()
				.append(this.constraintID, rhs.constraintID)
				.append(this.membersOnInstance.get(0).getID(),
						rhs.membersOnInstance.get(0).getID()).isEquals();
	}
}
