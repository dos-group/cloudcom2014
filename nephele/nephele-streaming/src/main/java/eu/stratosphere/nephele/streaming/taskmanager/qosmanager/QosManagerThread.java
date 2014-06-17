package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.AbstractQosMessage;
import eu.stratosphere.nephele.streaming.message.ChainUpdates;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.message.qosreport.QosReport;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.BufferSizeManager;

/**
 * Implements a thread that serves as a Qos manager. It is started by invoking
 * {@link Thread#start()} and can by shut down with {@link #shutdown()}. It
 * continuously processes {@link AbstractStreamMessage} objects from a
 * threadsafe queue and triggers Qos actions if necessary.
 * {@link #handOffStreamingData(AbstractStreamMessage)} can be used to enqueue
 * data.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosManagerThread extends Thread {

	private static final Log LOG = LogFactory.getLog(QosManagerThread.class);

	private final LinkedBlockingQueue<AbstractQosMessage> streamingDataQueue;

	private BufferSizeManager bufferSizeManager;

	private QosModel qosModel;

	public QosManagerThread(JobID jobID) {
		this.qosModel = new QosModel(jobID);
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractQosMessage>();
		this.bufferSizeManager = new BufferSizeManager(jobID, this.qosModel);
		this.setName(String.format("QosManagerThread (JobID: %s)",
				jobID.toString()));
	}

	@Override
	public void run() {
		LOG.info("Started Qos manager thread.");

		int nooOfReports = 0;
		int noOfEdgeLatencies = 0;
		int noOfVertexLatencies = 0;
		int noOfEdgeStatistics = 0;
		int noOfVertexAnnounces = 0;
		int noOfEdgeAnnounces = 0;

		try {
			while (!interrupted()) {
				AbstractQosMessage streamingData = this.streamingDataQueue
						.take();

				nooOfReports++;

				if (streamingData instanceof QosReport) {
					QosReport qosReport = (QosReport) streamingData;
					this.qosModel.processQosReport(qosReport);
					noOfEdgeLatencies += qosReport.getEdgeLatencies().size();
					noOfVertexLatencies += qosReport.getVertexLatencies()
							.size();
					noOfEdgeStatistics += qosReport.getEdgeStatistics().size();
					noOfVertexAnnounces += qosReport
							.getVertexQosReporterAnnouncements().size();
					noOfEdgeAnnounces += qosReport
							.getEdgeQosReporterAnnouncements().size();
					nooOfReports++;
				} else if (streamingData instanceof DeployInstanceQosRolesAction) {
					this.qosModel
							.mergeShallowQosGraph(((DeployInstanceQosRolesAction) streamingData)
									.getQosManager().getShallowQosGraph());
				} else if (streamingData instanceof ChainUpdates) {
					this.qosModel
							.processChainUpdates((ChainUpdates) streamingData);
				}

				long now = System.currentTimeMillis();
				if (this.qosModel.isReady()
						&& this.bufferSizeManager.isAdjustmentNecessary(now)) {

					this.bufferSizeManager.adjustBufferSizes();

					long buffersizeAdjustmentOverhead = System
							.currentTimeMillis() - now;
					LOG.debug(String
							.format("total messages: %d (edge: %d lats and %d stats | vertex: %d | edgeReporters: %d | vertexReporters: %d) || enqueued: %d || buffersizeAdjustmentOverhead: %d",
									nooOfReports, noOfEdgeLatencies,
									noOfEdgeStatistics, noOfVertexLatencies,
									noOfEdgeAnnounces, noOfVertexAnnounces,
									this.streamingDataQueue.size(),
									buffersizeAdjustmentOverhead));

					nooOfReports = 0;
					noOfEdgeLatencies = 0;
					noOfVertexLatencies = 0;
					noOfEdgeStatistics = 0;
					noOfEdgeAnnounces = 0;
					noOfVertexAnnounces = 0;
				}
			}

		} catch (InterruptedException e) {
		} finally {
			this.cleanUp();
		}

		LOG.info("Stopped Qos Manager thread");
	}

	private void cleanUp() {
		this.streamingDataQueue.clear();
		this.qosModel = null;
		this.bufferSizeManager = null;
	}

	public void shutdown() {
		this.interrupt();
	}

	public void handOffStreamingData(AbstractQosMessage data) {
		this.streamingDataQueue.add(data);
	}
}
