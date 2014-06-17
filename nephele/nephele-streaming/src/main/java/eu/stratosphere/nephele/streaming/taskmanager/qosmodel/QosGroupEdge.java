package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import eu.stratosphere.nephele.io.DistributionPattern;

/**
 * This class models a Qos group edge as part of a Qos graph. It is equivalent
 * to an {@link eu.stratosphere.nephele.executiongraph.ExecutionGroupEdge},
 * which in turn is equivalent to a
 * {@link eu.stratosphere.nephele.jobgraph.JobEdge}.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosGroupEdge {

	private DistributionPattern distributionPattern;

	private int outputGateIndex;

	private int inputGateIndex;

	private QosGroupVertex sourceVertex;

	private QosGroupVertex targetVertex;

	public QosGroupEdge(DistributionPattern distributionPattern,
			QosGroupVertex sourceVertex, QosGroupVertex targetVertex,
			int outputGateIndex, int inputGateIndex) {

		this.distributionPattern = distributionPattern;
		this.sourceVertex = sourceVertex;
		this.targetVertex = targetVertex;
		this.outputGateIndex = outputGateIndex;
		this.inputGateIndex = inputGateIndex;
		this.sourceVertex.setForwardEdge(this);
		this.targetVertex.setBackwardEdge(this);
	}

	public DistributionPattern getDistributionPattern() {
		return this.distributionPattern;
	}

	public QosGroupVertex getSourceVertex() {
		return this.sourceVertex;
	}

	public QosGroupVertex getTargetVertex() {
		return this.targetVertex;
	}

	public int getOutputGateIndex() {
		return this.outputGateIndex;
	}

	public int getInputGateIndex() {
		return this.inputGateIndex;
	}
}
