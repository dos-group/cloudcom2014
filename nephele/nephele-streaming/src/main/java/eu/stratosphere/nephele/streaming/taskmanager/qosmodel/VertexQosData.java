package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

/**
 * Instances of this class hold Qos data (currently only latency) of a
 * {@link QosVertex}. Vertex latency is only measured for those input/output
 * gate combinations that are covered by a Qos constraint.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class VertexQosData {

	private QosVertex vertex;

	/**
	 * Sparse array indexed by (inputGateIndex, outputGateIndex) of the vertex.
	 * Sparseness means that some (inputGateIndex, outputGateIndex) of the array
	 * may be null.
	 */
	private QosStatistic[][] qosStatistics;

	private final static int DEFAULT_NO_OF_STATISTICS_ENTRIES = 4;

	public VertexQosData(QosVertex vertex) {
		this.vertex = vertex;
		this.qosStatistics = new QosStatistic[1][1];
	}

	public QosVertex getVertex() {
		return this.vertex;
	}

	public double getLatencyInMillis(int inputGateIndex, int outputGateIndex) {
		QosStatistic statistic = this.qosStatistics[inputGateIndex][outputGateIndex];

		if (statistic.hasValues()) {
			return statistic.getArithmeticMean();
		}
		return -1;
	}

	public boolean isActive(int inputGateIndex, int outputGateIndex) {
		return this.qosStatistics[inputGateIndex][outputGateIndex].hasValues();
	}

	public void prepareForReportsOnGateCombination(int inputGateIndex,
			int outputGateIndex) {

		if (this.qosStatistics.length <= inputGateIndex) {
			QosStatistic[][] newArray = new QosStatistic[inputGateIndex + 1][];
			System.arraycopy(this.qosStatistics, 0, newArray, 0,
					this.qosStatistics.length);
            this.qosStatistics = newArray;
		}

		if (this.qosStatistics[inputGateIndex] == null) {
			this.qosStatistics[inputGateIndex] = new QosStatistic[outputGateIndex + 1];
		}

		if (this.qosStatistics[inputGateIndex].length <= outputGateIndex) {
			QosStatistic[] newArray = new QosStatistic[outputGateIndex + 1];
			System.arraycopy(this.qosStatistics[inputGateIndex], 0, newArray,
					0, this.qosStatistics[inputGateIndex].length);
            this.qosStatistics[inputGateIndex] = newArray;
		}

		if (this.qosStatistics[inputGateIndex][outputGateIndex] == null) {
			this.qosStatistics[inputGateIndex][outputGateIndex] = new QosStatistic(
					DEFAULT_NO_OF_STATISTICS_ENTRIES);
		}
	}

    /**
     * Adds a latency measurement consisting of a timestamp and the actual latency.
     *
     * Please note: Before submitting data to this method
     * {#prepareForReportsOnGateCombination} has to be called to prepare
     * internal data structures.
     *
     * @param inputGateIndex the input gate index
     * @param outputGateIndex the output gate index
     * @param timestamp the current timestamp as a long
     * @param latencyInMillis the current latency in milliseconds as a double
     */
	public void addLatencyMeasurement(int inputGateIndex, int outputGateIndex,
			long timestamp, double latencyInMillis) {

		QosValue value = new QosValue(latencyInMillis, timestamp);
		this.qosStatistics[inputGateIndex][outputGateIndex].addValue(value);
	}
}
