package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.util.ArrayList;

import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGate.GateType;

/**
 * This class models a Qos vertex as part of a Qos graph. It is equivalent to an
 * {@link eu.stratosphere.nephele.executiongraph.ExecutionVertex}.
 * 
 * Instances of this class contain sparse lists of input and output gates.
 * Sparseness means that, for example if the vertex has an output gate with
 * index 1 it may not have an output gate with index 0. This stems from the
 * fact, that the Qos graph itself only contains those group vertices and group
 * edges from the execution graph, that are covered by a constraint.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosVertex implements QosGraphMember {

	private QosGroupVertex groupVertex;

	private ExecutionVertexID vertexID;

	private InstanceConnectionInfo executingInstance;

	private ArrayList<QosGate> inputGates;

	private ArrayList<QosGate> outputGates;

	private int memberIndex;

	private String name;

	/**
	 * Only for use on the task manager side. Will not be transferred.
	 */
	private transient VertexQosData qosData;

	public QosVertex(ExecutionVertexID vertexID, String name,
			InstanceConnectionInfo executingInstance, int memberIndex) {

		this.vertexID = vertexID;
		this.name = name;
		this.executingInstance = executingInstance;
		this.inputGates = new ArrayList<QosGate>();
		this.outputGates = new ArrayList<QosGate>();
		this.memberIndex = memberIndex;
	}

	public ExecutionVertexID getID() {
		return this.vertexID;
	}

	public QosGate getInputGate(int gateIndex) {
		try {
			return this.inputGates.get(gateIndex);
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

	public void setInputGate(QosGate inputGate) {
		if (inputGate.getGateIndex() >= this.inputGates.size()) {
			this.fillWithNulls(this.inputGates, inputGate.getGateIndex() + 1);
		}

		inputGate.setVertex(this);
		inputGate.setGateType(GateType.INPUT_GATE);
		this.inputGates.set(inputGate.getGateIndex(), inputGate);
	}

	public QosGate getOutputGate(int gateIndex) {
		try {
			return this.outputGates.get(gateIndex);
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

	public void setOutputGate(QosGate outputGate) {
		if (outputGate.getGateIndex() >= this.outputGates.size()) {
			this.fillWithNulls(this.outputGates, outputGate.getGateIndex() + 1);
		}

		outputGate.setVertex(this);
		outputGate.setGateType(GateType.OUTPUT_GATE);
		this.outputGates.set(outputGate.getGateIndex(), outputGate);
	}

	private <T> void fillWithNulls(ArrayList<T> list, int targetSize) {
		int toAdd = targetSize - list.size();

		for (int i = 0; i < toAdd; i++) {
			list.add(null);
		}
	}

	public InstanceConnectionInfo getExecutingInstance() {
		return this.executingInstance;
	}

	public VertexQosData getQosData() {
		return this.qosData;
	}

	public void setQosData(VertexQosData qosData) {
		this.qosData = qosData;
	}

	public String getName() {
		return this.name;
	}

	@Override
	public String toString() {
		return this.name;
	}

	public QosVertex cloneWithoutGates() {
		QosVertex clone = new QosVertex(this.vertexID, this.name,
				this.executingInstance, this.memberIndex);
		return clone;
	}

	public void setMemberIndex(int memberIndex) {
		this.memberIndex = memberIndex;
	}

	public int getMemberIndex() {
		return this.memberIndex;
	}

	public void setGroupVertex(QosGroupVertex qosGroupVertex) {
		this.groupVertex = qosGroupVertex;
	}

	public QosGroupVertex getGroupVertex() {
		return this.groupVertex;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.vertexID.hashCode();
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
		QosVertex rhs = (QosVertex) obj;
		return this.vertexID.equals(rhs.vertexID);
	}

	public static QosVertex fromExecutionVertex(ExecutionVertex executionVertex) {
		return new QosVertex(executionVertex.getID(), executionVertex.getName()
				+ executionVertex.getIndexInVertexGroup(), executionVertex
				.getAllocatedResource().getInstance()
				.getInstanceConnectionInfo(),
				executionVertex.getIndexInVertexGroup());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphMember
	 * #isVertex()
	 */
	@Override
	public boolean isVertex() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphMember
	 * #isEdge()
	 */
	@Override
	public boolean isEdge() {
		return false;
	}
}
