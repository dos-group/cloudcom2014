package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosSequenceLatencySummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class QosConstraintSummary implements IOReadableWritable {

	private LatencyConstraintID constraintID;

	private long latencyConstraintMillis;

	/**
	 * Each subarray aggregates data for a {@link QosGroupVertex} or a
	 * {@link QosGroupEdge}.
	 *
	 * @see #getAggregatedMemberStatistics()
	 */
	private double[][] aggregatedMemberStats;

	private double minTotalLatency;

	private double aggregatedTotalLatency;

	private double maxTotalLatency;

	private int noOfSequences;

	private int noOfSequencesBelowConstraint;

	private int noOfSequencesAboveConstraint;

	private int[] taskDop;

	private boolean isFinalized;
	
	public QosConstraintSummary() {
	}
	
	public QosConstraintSummary(JobGraphLatencyConstraint constraint) {
		this(constraint.getID(),
				constraint.getLatencyConstraintInMillis(),
				constraint.getSequence().size(), 
				constraint.getSequence().getFirst().isVertex());
	}

	public QosConstraintSummary(LatencyConstraintID constraintID,
			long latencyConstraintMillis,
			int sequenceLength, 
			boolean sequenceStartsWithVertex) {
		
		this.constraintID = constraintID;
		this.latencyConstraintMillis = latencyConstraintMillis;
		
		createMemberStatsArray(sequenceLength, sequenceStartsWithVertex);
		initState();
	}

	private void createMemberStatsArray(int sequenceLength, boolean sequenceStartsWithVertex) {
		this.aggregatedMemberStats = new double[sequenceLength][];

		boolean nextIsVertex = sequenceStartsWithVertex;
		for (int i = 0; i < sequenceLength; i++) {
			if (nextIsVertex) {
				this.aggregatedMemberStats[i] = new double[] { 0, 0, 0, 0 };
			} else {
				this.aggregatedMemberStats[i] = new double[] { 0, 0, 0, 0, 0, 0 };
			}
			nextIsVertex = !nextIsVertex;
		}
	}

	public void addQosSequenceLatencySummary(
			QosSequenceLatencySummary sequenceSummary) {
		if (this.isFinalized) {
			throw new RuntimeException(
					"Cannot add sequence to already finalized summary. This is a bug.");
		}

		double[][] memberStats = sequenceSummary.getMemberLatencies();
		for (int i = 0; i < memberStats.length; i++) {
			for (int j = 0; j < memberStats[i].length; j++) {
				this.aggregatedMemberStats[i][j] += memberStats[i][j];
			}
		}

		double sequenceLatency = sequenceSummary.getSequenceLatency();
		this.aggregatedTotalLatency += sequenceLatency;

		if (sequenceLatency < this.minTotalLatency) {
			this.minTotalLatency = sequenceLatency;
		}

		if (sequenceLatency > this.maxTotalLatency) {
			this.maxTotalLatency = sequenceLatency;
		}

		this.noOfSequences++;

		double constraintViolatedByMillis = sequenceLatency
				- latencyConstraintMillis;

		// only count violations of >5% of the constraint
		if (Math.abs(constraintViolatedByMillis) / latencyConstraintMillis > 0.05) {
			if (constraintViolatedByMillis > 0) {
				this.noOfSequencesAboveConstraint++;
			} else {
				this.noOfSequencesBelowConstraint++;
			}
		}
	}

	public void setGroupEdgeConsumptionRate(int indexInSequence,
			double totalConsumptionRate, int noOfConsumingVertices) {

		this.aggregatedMemberStats[indexInSequence][3] = totalConsumptionRate;
		this.aggregatedMemberStats[indexInSequence][5] = noOfConsumingVertices;
	}
	
	public void setGroupEdgeEmissionRate(int indexInSequence,
			double totalEmissionRate, int noOfEmittingVertices) {
		
		this.aggregatedMemberStats[indexInSequence][2] = totalEmissionRate;
		this.aggregatedMemberStats[indexInSequence][4] = noOfEmittingVertices;
	}

	/** @see #getTaskDop() */
	public void setTaskDop(int[] taskDop) {
		this.taskDop = taskDop;
	}

	public void mergeOtherSummary(QosConstraintSummary constraintSummary) {
		if (this.isFinalized) {
			throw new RuntimeException(
					"Cannot add sequence to already finalized summary. This is a bug.");
		}

		boolean nextIsVertex = constraintSummary.doesSequenceStartWithVertex();

		int noOfSummarizedSequences = constraintSummary.getNoOfSequences();
		double[][] memberStats = constraintSummary
				.getAggregatedMemberStatistics();
		for (int i = 0; i < memberStats.length; i++) {
			for (int j = 0; j < memberStats[i].length; j++) {
				if (nextIsVertex) {
					aggregatedMemberStats[i][j] += noOfSummarizedSequences * memberStats[i][j];
				} else {
					if (j < 2) {
						// apply a weighting proportional to noOfSummarizedSequences
						// to compute average latencies
						this.aggregatedMemberStats[i][j] += noOfSummarizedSequences
								* memberStats[i][j];
					} else if (j == 2 || j == 3) {
						// record emission/consumption rates are weighted with their
						// number of vertices
						this.aggregatedMemberStats[i][j] += memberStats[i][j+2] * memberStats[i][j];
					} else {
						// vertex counts are just added up
						this.aggregatedMemberStats[i][j] += memberStats[i][j];
					}
				}
			}
			nextIsVertex = !nextIsVertex;
		}

		// apply a weighting proportional to noOfSummarizedSequences
		this.aggregatedTotalLatency += noOfSummarizedSequences
				* constraintSummary.getAvgSequenceLatency();
		this.noOfSequences += noOfSummarizedSequences;
		this.noOfSequencesAboveConstraint += constraintSummary
				.getNoOfSequencesAboveConstraint();
		this.noOfSequencesBelowConstraint += constraintSummary
				.getNoOfSequencesBelowConstraint();

		if (constraintSummary.getMinSequenceLatency() < this.minTotalLatency) {
			this.minTotalLatency = constraintSummary.getMinSequenceLatency();
		}

		if (constraintSummary.getMaxSequenceLatency() > this.maxTotalLatency) {
			this.maxTotalLatency = constraintSummary.getMaxSequenceLatency();
		}
	}

	private void ensureIsFinalized() {
		if (!this.isFinalized) {
			if (this.noOfSequences > 0) {
				this.aggregatedTotalLatency /= this.noOfSequences;

				boolean nextIsVertex = this.doesSequenceStartWithVertex();

				for (int i = 0; i < getSequenceLength(); i++) {
					for (int j = 0; j < this.aggregatedMemberStats[i].length; j++) {
						if (nextIsVertex) {
							this.aggregatedMemberStats[i][j] /= this.noOfSequences;
						} else {
							if (j < 2) {
								// latencies have previously been weighted
								// with their number of sequences, now we compute the average
								// over all sequences
								this.aggregatedMemberStats[i][j] /= this.noOfSequences;
							} else if (j == 2 || j == 3) {
								// record emission/consumption rates have previously been weighted
								// with their number of vertices, no we compute the avg per
								// vertex
								this.aggregatedMemberStats[i][j] = aggregatedMemberStats[i][j]
										/ aggregatedMemberStats[i][j + 2];
							}
						}
					}

					nextIsVertex = !nextIsVertex;
				}
			} else {
				this.aggregatedTotalLatency = 0;
				this.minTotalLatency = 0;
				this.maxTotalLatency = 0;
				this.noOfSequencesAboveConstraint = 0;
				this.noOfSequencesBelowConstraint = 0;
			}
		}
		this.isFinalized = true;
	}
	
	public LatencyConstraintID getLatencyConstraintID() {
		return constraintID;
	}

	public long getLatencyConstraintMillis() {
		return latencyConstraintMillis;
	}
	
	private boolean doesSequenceStartWithVertex() {
		return this.aggregatedMemberStats[0].length == 4;
	}

	public int getSequenceLength() {
		return this.aggregatedMemberStats.length;
	}

	/**
	 * <p>
	 * Each subarray aggregates data for a {@link QosGroupVertex} or a
	 * {@link QosGroupEdge}.
	 * </p>
	 * 
	 * <p>
	 * Subarrays aggregating a group vertex contain four elements:
	 * </p>
	 *
	 * <p>
	 * (0) = The average vertex latency of the group vertex's active {@link QosVertex} members.
	 * </p>
	 *
	 * <p>
	 * (1) = The average vertex latency variance of the group vertex's active {@link QosVertex} members.
	 * </p>
	 *
	 * <p>
	 * (2) = The average inter arrival time of the group vertex's active {@link QosVertex} members.
	 * </p>
	 *
	 * <p>
	 * (3) = The average inter arrival time variance of the group vertex's active {@link QosVertex} members.
	 * </p>
	 *
	 * <p>
	 * Subarrays aggregating a group edge contain six elements:
	 * </p>
	 * 
	 * <p>
	 * (0) = The average output buffer latency the group edge's {@link QosEdge} members.
	 * </p>
	 * 
	 * <p>
	 * (1) = The average remaining transport latency (i.e. without output buffer
	 * latency) of the group edge's {@link QosEdge} members. 
	 * </p>
	 * 
	 * <p>
	 * (2) = The average number of records per second that the source (member) vertices
	 * write into the group edge's {@link QosEdge} member edges. 
	 * </p>
	 * 
	 * <p>
	 * (3) = The avg number of records per second that the target (member) vertices read
	 * from the group edge's {@link QosEdge} member edges.
	 * </p>
	 * 
	 * <p>
	 * (4) The number of source (member) vertices writing into the group edge's {@link QosEdge}
	 * member edges. This is actually an int but represented as a double.
	 * </p>
	 * 
	 * <p>
	 * (5) The number of target (member) vertices reading from the group edge's {@link QosEdge}
	 * member edges. This is actually an int but represented as a double.
	 * </p>
	 */
	public double[][] getAggregatedMemberStatistics() {
		ensureIsFinalized();
		return aggregatedMemberStats;
	}

	public double getMinSequenceLatency() {
		ensureIsFinalized();
		return minTotalLatency;
	}

	public double getAvgSequenceLatency() {
		ensureIsFinalized();
		return aggregatedTotalLatency;
	}

	public double getMaxSequenceLatency() {
		ensureIsFinalized();
		return maxTotalLatency;
	}

	public int getNoOfSequences() {
		return noOfSequences;
	}

	public int getNoOfSequencesAboveConstraint() {
		return noOfSequencesAboveConstraint;
	}

	public int getNoOfSequencesBelowConstraint() {
		return noOfSequencesBelowConstraint;
	}
	
	/**
	 * Contains task dop of #edges + 1 tasks.
	 *
	 * If sequence starts/ends with an edge, this array contains source/target
	 * task too (which is not part of this sequence).
	 */
	public int[] getTaskDop() {
		return taskDop;
	}
	
	public boolean hasData() {
		return noOfSequences > 0;
	}
	
	public void reset() {
		initState();

		for (int i = 0; i < aggregatedMemberStats.length; i++) {
			for (int j = 0; j < aggregatedMemberStats[i].length; j++) {
				aggregatedMemberStats[i][j] = 0;
			}
		}

		isFinalized = false;
	}

	private void initState() {
		noOfSequences = 0;
		noOfSequencesBelowConstraint = 0;
		noOfSequencesAboveConstraint = 0;
		aggregatedTotalLatency = 0;
		minTotalLatency = Double.MAX_VALUE;
		maxTotalLatency = Double.MIN_VALUE;
		taskDop = new int[0];
		isFinalized = false;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		ensureIsFinalized();
		
		this.constraintID.write(out);
		out.writeLong(latencyConstraintMillis);
		
		out.writeInt(getSequenceLength());
		out.writeBoolean(doesSequenceStartWithVertex());  
		
		for (int i = 0; i < getSequenceLength(); i++) {
			for (int j = 0; j < this.aggregatedMemberStats[i].length; j++) {
				out.writeDouble(aggregatedMemberStats[i][j]);
			}
		}
		
		out.writeDouble(minTotalLatency);
		out.writeDouble(maxTotalLatency);
		out.writeDouble(aggregatedTotalLatency);
		out.writeInt(noOfSequences);
		out.writeInt(noOfSequencesAboveConstraint);
		out.writeInt(noOfSequencesBelowConstraint);
	}

	@Override
	public void read(DataInput in) throws IOException {
		constraintID = new LatencyConstraintID();
		constraintID.read(in);
		latencyConstraintMillis = in.readLong();
		
		int sequenceLength = in.readInt();
		boolean sequenceStartsWithVertex = in.readBoolean();
		createMemberStatsArray(sequenceLength, sequenceStartsWithVertex);
		
		for (int i = 0; i < getSequenceLength(); i++) {
			for (int j = 0; j < this.aggregatedMemberStats[i].length; j++) {
				aggregatedMemberStats[i][j] = in.readDouble();
			}
		}
		
		this.minTotalLatency = in.readDouble();
		this.maxTotalLatency = in.readDouble();
		this.aggregatedTotalLatency = in.readDouble();
		this.noOfSequences = in.readInt();
		this.noOfSequencesAboveConstraint = in.readInt();
		this.noOfSequencesBelowConstraint = in.readInt();
		this.isFinalized = true;
	}
}
