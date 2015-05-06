package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.message.qosreport.EdgeStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReporterConfigCenter;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.TimestampTag;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.edge.OutputBufferLifetimeSampler;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.BernoulliSampleDesign;
import eu.stratosphere.nephele.streaming.util.StreamPluginConfig;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A instance of this class keeps track of and reports on the Qos statistics of
 * an output gate's output channels.
 *
 * This class is thread-safe.
 *
 * An {@link EdgeStatistics} record per output channel will be handed to the
 * provided {@link QosReportForwarderThread} approximately once per aggregation
 * interval (see {@link QosReporterConfigCenter}). "Approximately" because if no
 * records have been received/emitted, nothing will be reported.
 *
 *
 * @author Bjoern Lohrmann
 *
 */
public class OutputGateReporter extends CountingGateReporter {
	/**
	 * No need for a thread-safe set because it is only accessed in synchronized
	 * methods.
	 */
	private HashSet<QosReporterID> reporters;

	/**
	 * Maps from an output channels index in the runtime gate to the statistics
	 * reporter. This needs to be threadsafe because statistics collection may
	 * already be running while OutputChannelChannelStatisticsReporters are
	 * being added.
	 */
	private CopyOnWriteArrayList<OutputChannelChannelStatisticsReporter> reportersByChannelIndexInRuntimeGate;

	private QosReportForwarderThread reportForwarder;

	private ReportTimer reportTimer;

	public void initReporter(QosReportForwarderThread qosReporter, int noOfOutputChannels) {
		this.reportForwarder = qosReporter;
		this.reporters = new HashSet<QosReporterID>();
		this.reportersByChannelIndexInRuntimeGate = new CopyOnWriteArrayList<OutputChannelChannelStatisticsReporter>();
		Collections.addAll(this.reportersByChannelIndexInRuntimeGate,
				new OutputChannelChannelStatisticsReporter[noOfOutputChannels]);
		this.reportTimer = new ReportTimer(StreamPluginConfig.getAggregationIntervalMillis());
		setReporter(true);
	}

	public class OutputChannelChannelStatisticsReporter {

		private QosReporterID.Edge reporterID;
		private long timeOfLastReport;
		private long amountTransmittedAtLastReport;
		private long currentAmountTransmitted;
		private int recordsEmittedSinceLastReport;
		private int outputBuffersSentSinceLastReport;

		public final BernoulliSampleDesign recordTaggingSampleDesign;

		private final OutputBufferLifetimeSampler outputBufferLifetimeSampler;

		public OutputChannelChannelStatisticsReporter(
				QosReporterID.Edge reporterID) {

			this.reporterID = reporterID;
			this.timeOfLastReport = System.currentTimeMillis();
			this.amountTransmittedAtLastReport = 0;
			this.currentAmountTransmitted = 0;
			this.recordsEmittedSinceLastReport = 0;
			this.outputBuffersSentSinceLastReport = 0;
			
			this.recordTaggingSampleDesign = new BernoulliSampleDesign(
					OutputGateReporter.this.reportForwarder.getConfigCenter().getSamplingProbability() / 100.0);
			
			this.outputBufferLifetimeSampler = new OutputBufferLifetimeSampler(
					OutputGateReporter.this.reportForwarder.getConfigCenter().getSamplingProbability() / 100.0);
		}

		/**
		 * Returns the reporterID.
		 * 
		 * @return the reporterID
		 */
		public QosReporterID.Edge getReporterID() {
			return this.reporterID;
		}

		public void sendReportIfDue(long now) {
			if (this.reportIsDue()) {
				this.sendReport(now);
				this.reset(now);
			}
		}

		private boolean reportIsDue() {
			return this.recordsEmittedSinceLastReport > 0
					&& this.outputBuffersSentSinceLastReport > 0
					&& this.outputBufferLifetimeSampler.hasSample();
		}

		private void reset(long now) {
			this.timeOfLastReport = now;
			this.amountTransmittedAtLastReport = this.currentAmountTransmitted;
			this.recordsEmittedSinceLastReport = 0;
			this.outputBuffersSentSinceLastReport = 0;
			this.recordTaggingSampleDesign.reset();
			this.outputBufferLifetimeSampler.reset();
		}

		private void sendReport(long now) {

			double secsPassed = (now - this.timeOfLastReport) / 1000.0;
			double mbitPerSec = (this.currentAmountTransmitted - this.amountTransmittedAtLastReport)
					* 8 / (1000000.0 * secsPassed);
			double meanOutputBufferLifetime = outputBufferLifetimeSampler.getMeanOutputBufferLifetimeMillis();
			double recordsPerBuffer = (double) this.recordsEmittedSinceLastReport
					/ this.outputBuffersSentSinceLastReport;
			double recordsPerSecond = this.recordsEmittedSinceLastReport
					/ secsPassed;

			EdgeStatistics channelStatsMessage = new EdgeStatistics(
					this.reporterID, mbitPerSec, meanOutputBufferLifetime,
					recordsPerBuffer, recordsPerSecond);

			OutputGateReporter.this.reportForwarder
					.addToNextReport(channelStatsMessage);
		}

		public void updateStatsAndTagRecordIfNecessary(
				AbstractTaggableRecord record) {
			this.recordsEmittedSinceLastReport++;

			boolean shouldSample = recordTaggingSampleDesign.shouldSample();
			if (shouldSample) {
				this.tagRecord(record);
			} else {
				record.setTag(null);
			}
		}

		private void tagRecord(AbstractTaggableRecord record) {
			TimestampTag tag = new TimestampTag();
			tag.setTimestamp(System.currentTimeMillis());
			record.setTag(tag);
		}

		public void outputBufferSent(long currentAmountTransmitted) {
			this.outputBuffersSentSinceLastReport++;
			this.currentAmountTransmitted = currentAmountTransmitted;
			this.outputBufferLifetimeSampler.outputBufferSent();
		}

		public void outputBufferAllocated() {
			this.outputBufferLifetimeSampler.outputBufferAllocated();
		}
	}


	public void recordEmitted(int channelIndex, AbstractTaggableRecord record) {
		OutputChannelChannelStatisticsReporter outputChannelReporter = this.reportersByChannelIndexInRuntimeGate
				.get(channelIndex);

		if (outputChannelReporter != null) {
			outputChannelReporter.updateStatsAndTagRecordIfNecessary(record);
		}
	}

	public void outputBufferSent(int runtimeGateChannelIndex,
			long currentAmountTransmitted) {

		OutputChannelChannelStatisticsReporter reporter = this.reportersByChannelIndexInRuntimeGate
				.get(runtimeGateChannelIndex);


		if (reporter != null) {
			reporter.outputBufferSent(currentAmountTransmitted);
		}
	}

	public void outputBufferAllocated(int runtimeGateChannelIndex) {
		OutputChannelChannelStatisticsReporter reporter = this.reportersByChannelIndexInRuntimeGate
						.get(runtimeGateChannelIndex);

		long now = System.currentTimeMillis();
		if (reporter != null) {
			reporter.outputBufferAllocated();
		}
		sendReportsIfDue(now);
	}

	private void sendReportsIfDue(long now) {
		if (reportTimer.reportIsDue()) {
			for (OutputChannelChannelStatisticsReporter reporter : reportersByChannelIndexInRuntimeGate) {
				if (reporter != null) {
					reporter.sendReportIfDue(now);
				}
			}
		}
		reportTimer.reset(now);
	}

	public synchronized void addEdgeQosReporterConfig(
			int channelIndexInRuntimeGate, QosReporterID.Edge reporterID) {

		if (this.reporters.contains(reporterID)) {
			return;
		}

		OutputChannelChannelStatisticsReporter channelStats = new OutputChannelChannelStatisticsReporter(
				reporterID);

		this.reportersByChannelIndexInRuntimeGate.set(
				channelIndexInRuntimeGate, channelStats);
		this.reporters.add(reporterID);
	}
}
