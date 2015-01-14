package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.SamplingStrategy;
import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReporterConfigCenter;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.TimestampTag;
import eu.stratosphere.nephele.streaming.util.StreamUtil;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Handles the measurement and reporting of latencies and record
 * consumption/emission rates for a particular vertex. Such a latency is defined
 * as the timespan between record receptions and emits on a particular
 * input/output gate combination of the vertex. Thus one vertex may have
 * multiple associated latencies, one for each input/output gate combination.
 * Which gate combination is measured and reported on must be configured by
 * calling
 * {@link #addReporter(int, int, eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID.Vertex,
 * eu.stratosphere.nephele.streaming.SamplingStrategy)}
 * .
 * <p/>
 * An {@link VertexStatistics} record per configured input/output gate
 * combination will be handed to the provided {@link QosReportForwarderThread}
 * approximately once per aggregation interval (see
 * {@link QosReporterConfigCenter}). "Approximately" because if no records have
 * been received/emitted, nothing will be reported.
 *
 * @author Bjoern Lohrmann
 */
public class StatisticsReportManager {

	// private static final Log LOG = LogFactory.getLog(TaskLatencyReporter.class);

	private final QosReportForwarderThread reportForwarder;

	/**
	 * For each input gate of the task for whose channels latency reporting is
	 * required, this list contains a InputGateReporterManager. A
	 * InputGateReporterManager keeps track of and reports on the latencies for
	 * all of the input gate's channels. This is a sparse list (may contain
	 * nulls), indexed by the runtime gate's own indices.
	 */
	private final AtomicReferenceArray<InputGateReporter> inputGateReporter;

	/**
	 * For each output gate of the task for whose output channels QoS statistics
	 * are required (throughput, output buffer lifetime, ...), this list
	 * contains a OutputGateReporterManager. Each OutputGateReporterManager
	 * keeps track of and reports on Qos statistics all of the output gate's
	 * channels and also attaches tags to records sent via its channels. This is
	 * a sparse list (may contain nulls), indexed by the runtime gate's own
	 * indices.
	 */
	private final AtomicReferenceArray<OutputGateReporter> outputGateReporter;

	private final ConcurrentHashMap<QosReporterID, VertexQosReporter> reporters;
	private final AtomicReferenceArray<VertexQosReporter[]> reportersByInputGate;
	private final AtomicReferenceArray<VertexQosReporter[]> reportersByOutputGate;

	public StatisticsReportManager(QosReportForwarderThread qosReporter,
			int noOfInputGates, int noOfOutputGates) {

		this.reportForwarder = qosReporter;

		this.inputGateReporter = new AtomicReferenceArray<InputGateReporter>(
				noOfInputGates);
		this.outputGateReporter = new AtomicReferenceArray<OutputGateReporter>(
				noOfOutputGates);

		this.reportersByInputGate = StreamUtil
				.createAtomicReferenceArrayOfEmptyArrays(
						VertexQosReporter.class, noOfInputGates);
		this.reportersByOutputGate = StreamUtil
				.createAtomicReferenceArrayOfEmptyArrays(
						VertexQosReporter.class, noOfOutputGates);

		this.reporters = new ConcurrentHashMap<QosReporterID, VertexQosReporter>();
	}

	public void recordReceived(int runtimeInputGateIndex) {
		InputGateReporter igCounter = inputGateReporter
				.get(runtimeInputGateIndex);

		if (igCounter != null) {
			igCounter.countRecord();
		}

		for (VertexQosReporter reporter : this.reportersByInputGate
				.get(runtimeInputGateIndex)) {
			reporter.recordReceived(runtimeInputGateIndex);
		}
	}

	public void tryingToReadRecord(int runtimeInputGateIndex) {
		for (VertexQosReporter reporter : this.reportersByInputGate
				.get(runtimeInputGateIndex)) {
			reporter.tryingToReadRecord(runtimeInputGateIndex);
		}
	}

	public void inputBufferConsumed(int inputGateIndex, int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {

		for (VertexQosReporter reporter : this.reportersByInputGate
				.get(inputGateIndex)) {
			reporter.inputBufferConsumed(inputGateIndex, channelIndex,
					bufferInterarrivalTimeNanos, recordsReadFromBuffer);
		}
	}

	public void reportLatenciesIfNecessary(int inputGateIndex, int channelIndex, TimestampTag timestampTag) {
		InputGateReporter inputGateReporter = this.inputGateReporter.get(inputGateIndex);
		if (inputGateReporter.isReporter()) {
			inputGateReporter.reportLatenciesIfNecessary(channelIndex, timestampTag);
		}
	}

	public void outputBufferSent(int outputGateIndex, int channelIndex, long currentAmountTransmitted) {
		OutputGateReporter outputGateReporter = this.outputGateReporter.get(outputGateIndex);
		if (outputGateReporter.isReporter()) {
			outputGateReporter.outputBufferSent(channelIndex, currentAmountTransmitted);
		}
	}

	public void recordEmitted(int outputGateIndex, int channelIndex, AbstractTaggableRecord record) {
		OutputGateReporter ogStats = outputGateReporter.get(outputGateIndex);

		if (ogStats != null) {
			ogStats.countRecord();
			if (ogStats.isReporter()) {
				ogStats.recordEmitted(channelIndex, record);
			}
		}

		for (VertexQosReporter reporter : reportersByOutputGate.get(outputGateIndex)) {
			reporter.recordEmitted(outputGateIndex);
		}
	}

	public void outputBufferAllocated(int runtimeGateIndex, int channelIndex) {
		OutputGateReporter outputGateReporter = this.outputGateReporter.get(runtimeGateIndex);
		if (outputGateReporter != null) {
			outputGateReporter.outputBufferAllocated(channelIndex);
		}
	}

	public boolean containsReporter(QosReporterID.Vertex reporterID) {
		return this.reporters.containsKey(reporterID);
	}

	public synchronized void addReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID,
			SamplingStrategy samplingStrategy) {

		if (this.reporters.containsKey(reporterID)) {
			return;
		}

		if (!reporterID.isDummy()) {
			inputGateReporter.compareAndSet(runtimeInputGateIndex, null,
					new InputGateReporter());
			outputGateReporter.compareAndSet(runtimeOutputGateIndex, null,
					new OutputGateReporter());

			switch (samplingStrategy) {
			case READ_WRITE:
				addReadWriteReporter(runtimeInputGateIndex,
						runtimeOutputGateIndex, reporterID);
				break;

			case READ_READ:
				addReadReadReporter(runtimeInputGateIndex,
						runtimeOutputGateIndex, reporterID);
				break;
			default:
				throw new IllegalArgumentException(
						"Unsupported sampling strategy: " + samplingStrategy);
			}

		} else if (runtimeInputGateIndex != -1) {
			inputGateReporter.compareAndSet(runtimeInputGateIndex, null,
					new InputGateReporter());
			addVertexConsumptionReporter(runtimeInputGateIndex, reporterID);

		} else if (runtimeOutputGateIndex != -1) {
			outputGateReporter.compareAndSet(runtimeOutputGateIndex, null,
					new OutputGateReporter());
			addVertexEmissionReporter(runtimeOutputGateIndex, reporterID);
		}
	}

	private void addVertexEmissionReporter(int runtimeOutputGateIndex,
			QosReporterID.Vertex reporterID) {
		VertexEmissionReporter reporter = new VertexEmissionReporter(
				reportForwarder, reporterID, runtimeOutputGateIndex,
				outputGateReporter.get(runtimeOutputGateIndex));

		addToReporterArray(reportersByOutputGate, runtimeOutputGateIndex,
				reporter);
		this.reporters.put(reporterID, reporter);
	}

	private void addVertexConsumptionReporter(int runtimeInputGateIndex,
			QosReporterID.Vertex reporterID) {
		VertexConsumptionReporter reporter = new VertexConsumptionReporter(
				reportForwarder, reporterID, runtimeInputGateIndex,
				inputGateReporter.get(runtimeInputGateIndex));

		addToReporterArray(reportersByInputGate, runtimeInputGateIndex,
				reporter);
		this.reporters.put(reporterID, reporter);
	}

	public void addReadReadReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID) {

		// search for ReadReadVertexQosReporterGroup
		ReadReadVertexQosReporterGroup groupReporter = null;
		for (VertexQosReporter vertexQosReporter : reportersByInputGate
				.get(runtimeInputGateIndex)) {
			if (vertexQosReporter instanceof ReadReadVertexQosReporterGroup) {
				groupReporter = (ReadReadVertexQosReporterGroup) vertexQosReporter;
				break;
			}
		}

		// create a ReadReadVertexQosReporterGroup if none found for the given
		// runtimeInputGateIndex
		if (groupReporter == null) {
			groupReporter = new ReadReadVertexQosReporterGroup(reportForwarder, runtimeInputGateIndex,
					inputGateReporter.get(runtimeInputGateIndex));
			// READ_READ reporters needs to keep track of all input gates
			for (int i = 0; i < this.reportersByInputGate.length(); i++) {
				addToReporterArray(reportersByInputGate, i, groupReporter);
			}
		}

		ReadReadReporter reporter = new ReadReadReporter(reportForwarder,
				reporterID, groupReporter.getReportTimer(),
				runtimeInputGateIndex, runtimeOutputGateIndex,
				inputGateReporter.get(runtimeInputGateIndex),
				outputGateReporter.get(runtimeOutputGateIndex));

		groupReporter.addReporter(reporter);
		this.reporters.put(reporterID, reporter);
	}

	public void addReadWriteReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID) {

		VertexQosReporter reporter = new ReadWriteReporter(reportForwarder,
				reporterID, runtimeInputGateIndex, runtimeOutputGateIndex,
				inputGateReporter.get(runtimeInputGateIndex),
				outputGateReporter.get(runtimeOutputGateIndex));

		// for the READ_WRITE strategy the reporter needs to keep track of the
		// events on
		// all input gates
		for (int igIndex = 0; igIndex < this.reportersByInputGate.length(); igIndex++) {
			addToReporterArray(reportersByInputGate, igIndex, reporter);
		}
		// for the READ_WRITE strategy the reporter needs to keep track of the
		// events on
		// all output gates, too
		for (int ogIndex = 0; ogIndex < this.reportersByOutputGate.length(); ogIndex++) {
			addToReporterArray(reportersByOutputGate, ogIndex, reporter);
		}
		this.reporters.put(reporterID, reporter);
	}

	private void addToReporterArray(
			AtomicReferenceArray<VertexQosReporter[]> reporterArray,
			int gateIndex, VertexQosReporter reporter) {

		reporterArray.set(gateIndex,
				StreamUtil.appendToArrayAt(reporterArray.get(gateIndex),
						VertexQosReporter.class, reporter));
	}

	public synchronized void addInputGateReporter(int inputGateIndex, int channelIndex,
			int noOfInputChannels, QosReporterID.Edge reporterID) {
		inputGateReporter.compareAndSet(inputGateIndex, null, new InputGateReporter());
		InputGateReporter inputGateReporter = this.inputGateReporter.get(inputGateIndex);
		inputGateReporter.initReporter(reportForwarder, noOfInputChannels);
		inputGateReporter.addEdgeQosReporterConfig(channelIndex, reporterID);
	}

	public synchronized void addOutputGateReporter(int outputGateIndex, int channelIndex,
			int noOfOutputChannels, QosReporterID.Edge reporterID) {
		outputGateReporter.compareAndSet(outputGateIndex, null, new OutputGateReporter());
		OutputGateReporter outputGateReporter = this.outputGateReporter.get(outputGateIndex);
		outputGateReporter.initReporter(reportForwarder, noOfOutputChannels);
		outputGateReporter.addEdgeQosReporterConfig(channelIndex, reporterID);
	}
}
