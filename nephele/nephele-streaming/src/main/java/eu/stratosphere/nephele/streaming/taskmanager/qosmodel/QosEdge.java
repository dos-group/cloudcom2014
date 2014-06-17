package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * This class models a Qos edge as part of a Qos graph. It is equivalent to an
 * {@link eu.stratosphere.nephele.executiongraph.ExecutionEdge}.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosEdge implements QosGraphMember {

	private final ChannelID sourceChannelID;

	private final ChannelID targetChannelID;

	private QosGate outputGate;

	private QosGate inputGate;

	/**
	 * The index of this edge in the output gate's list of edges.
	 */
	private int outputGateEdgeIndex;

	/**
	 * The index of this edge in the input gate's list of edges.
	 */
	private int inputGateEdgeIndex;

	/**
	 * Only for use on the task manager side. Will not be transferred.
	 */
	private transient EdgeQosData qosData;

	public QosEdge(ChannelID sourceChannelID, ChannelID targetChannelID,
			int outputGateEdgeIndex, int inputGateEdgeIndex) {

		this.sourceChannelID = sourceChannelID;
		this.targetChannelID = targetChannelID;
		this.outputGateEdgeIndex = outputGateEdgeIndex;
		this.inputGateEdgeIndex = inputGateEdgeIndex;
	}

	/**
	 * Returns the outputGate.
	 * 
	 * @return the outputGate
	 */
	public QosGate getOutputGate() {
		return this.outputGate;
	}

	/**
	 * Sets the outputGate to the specified value.
	 * 
	 * @param outputGate
	 *            the outputGate to set
	 */
	public void setOutputGate(QosGate outputGate) {
		this.outputGate = outputGate;
		this.outputGate.addEdge(this);
	}

	/**
	 * Returns the inputGate.
	 * 
	 * @return the inputGate
	 */
	public QosGate getInputGate() {
		return this.inputGate;
	}

	/**
	 * Sets the inputGate to the specified value.
	 * 
	 * @param inputGate
	 *            the inputGate to set
	 */
	public void setInputGate(QosGate inputGate) {
		this.inputGate = inputGate;
		this.inputGate.addEdge(this);
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

	public EdgeQosData getQosData() {
		return this.qosData;
	}

	public void setQosData(EdgeQosData qosData) {
		this.qosData = qosData;
	}

	@Override
	public String toString() {
		return String.format("%s->%s", this.getOutputGate().getVertex()
				.getName(), this.getInputGate().getVertex().getName());
	}

	public QosEdge cloneWithoutGates() {
		QosEdge clone = new QosEdge(this.sourceChannelID, this.targetChannelID,
				this.outputGateEdgeIndex, this.inputGateEdgeIndex);
		return clone;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.sourceChannelID.hashCode();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}

		QosEdge other = (QosEdge) obj;
		return this.sourceChannelID.equals(other.sourceChannelID);
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
		return false;
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
		return true;
	}
}
