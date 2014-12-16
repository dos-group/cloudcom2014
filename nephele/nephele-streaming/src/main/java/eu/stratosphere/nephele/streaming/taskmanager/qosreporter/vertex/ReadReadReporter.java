package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.Sample;

/**
 * @author Ilya Verbitskiy, Bjoern Lohrmann
 */
public class ReadReadReporter implements VertexQosReporter {

	private final QosReporterID.Vertex reporterID;

	private final QosReportForwarderThread reportForwarder;
	
	private final ReportTimer reportTimer;

	private long emitCounterAtLastReport;
	private final OutputGateEmitStatistics outputGateEmitCounter;

	private final int runtimeInputGateIndex;

	private final int runtimeOutputGateIndex;

	private boolean igIsChained;

	public ReadReadReporter(QosReportForwarderThread reportForwarder,
			QosReporterID.Vertex reporterID,
			ReportTimer reportTimer,
			int runtimeInputGateIndex,
			int runtimeOutputGateIndex,
			InputGateReceiveCounter igReceiveCounter,
			OutputGateEmitStatistics emitCounter) {

		this.reportForwarder = reportForwarder;
		this.reporterID = reporterID;
		this.reportTimer = reportTimer;
		
		this.runtimeInputGateIndex = runtimeInputGateIndex;
		this.runtimeOutputGateIndex = runtimeOutputGateIndex;

		this.igIsChained = false;

		emitCounterAtLastReport = emitCounter.getEmitted();
		this.outputGateEmitCounter = emitCounter;
	}
	
	public void sendReport(long now, 
			Sample vertexLatencyMillis,
			Sample interarrivalTimeMillis,
			double recordsConsumedPerSec) {
		
		if (igIsChained)
			throw new RuntimeException("sendReport called with interarrival time in chained mode. This is a bug.");
		else if(interarrivalTimeMillis == null)
			throw new RuntimeException("sendReport called without interarrival time in unchained mode. This is a bug.");

		double secsPassed = (now - reportTimer.getTimeOfLastReport()) / 1000.0;
		
		VertexStatistics toSend = new VertexStatistics(reporterID,
					vertexLatencyMillis,
					recordsConsumedPerSec,
					getRecordsEmittedPerSec(secsPassed),
					interarrivalTimeMillis);
		reportForwarder.addToNextReport(toSend);
	}

	public void sendReport(long now,
			Sample vertexLatencyMillis,
			double recordsConsumedPerSec) {

		if (!igIsChained)
			throw new RuntimeException("sendReport called with wrong arguments in unchained mode. This is a bug.");

		double secsPassed = (now - reportTimer.getTimeOfLastReport()) / 1000.0;

		VertexStatistics toSend = new VertexStatistics(reporterID,
					vertexLatencyMillis,
					recordsConsumedPerSec,
					getRecordsEmittedPerSec(secsPassed),
					null);
		reportForwarder.addToNextReport(toSend);
	}

	private double getRecordsEmittedPerSec(double secsPassed) {
		double recordEmittedPerSec = -1;
		if (outputGateEmitCounter != null) {
			recordEmittedPerSec = (outputGateEmitCounter.getEmitted() - emitCounterAtLastReport)
					/ secsPassed;
			emitCounterAtLastReport = outputGateEmitCounter.getEmitted();
		}
		return recordEmittedPerSec;
	}

	@Override
	public void setInputGateChained(boolean isChained) {
		this.igIsChained = isChained;
	}

	@Override
	public void setOutputGateChained(boolean isChained) {
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");
	}

	@Override
	public void tryingToReadRecord(int runtimeInputGateIndex) {
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");
	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");
	}


	@Override
	public void inputBufferConsumed(int inputGateIndex, int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");		
	}
	
	@Override
	public int getRuntimeInputGateIndex() {
		return runtimeInputGateIndex;
	}


	@Override
	public int getRuntimeOutputGateIndex() {
		return runtimeOutputGateIndex;
	}


	@Override
	public ReportTimer getReportTimer() {
		return reportTimer;
	}
}
