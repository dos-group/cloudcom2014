package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.plugins.PluginManager;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintViolationListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosModel;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosUtils;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeQosData;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphMember;
import eu.stratosphere.nephele.taskmanager.bufferprovider.GlobalBufferPool;

/**
 * Used by the Qos manager to manage the output buffer sizes in a Qos graph. It
 * uses a Qos model to search for sequences of Qos edges and vertices that
 * violate a Qos constraint, and then increases/decreases output buffer sizes
 * accordingly.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class BufferSizeManager {

	private static final Log LOG = LogFactory.getLog(BufferSizeManager.class);

	/**
	 * Provides access to the configuration entry which defines the buffer size
	 * adjustment interval-
	 */
	public static final String QOSMANAGER_ADJUSTMENTINTERVAL_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.adjustmentinterval");

	public static final long DEFAULT_ADJUSTMENTINTERVAL = 5000;

	public final static long WAIT_BEFORE_FIRST_ADJUSTMENT = 30 * 1000;

	public long adjustmentInterval;

	private QosModel qosModel;

	private StreamMessagingThread messagingThread;

	private long timeOfNextAdjustment;

	private int maximumBufferSize;

	private BufferSizeLogger bufferSizeLogger;

	private JobID jobID;

	public BufferSizeManager(JobID jobID, QosModel qosModel) {
		this.jobID = jobID;
		this.qosModel = qosModel;
		this.messagingThread = StreamMessagingThread.getInstance();

		this.adjustmentInterval = GlobalConfiguration.getLong(
				QOSMANAGER_ADJUSTMENTINTERVAL_KEY, DEFAULT_ADJUSTMENTINTERVAL);

		this.timeOfNextAdjustment = QosUtils.alignToInterval(
				System.currentTimeMillis() + WAIT_BEFORE_FIRST_ADJUSTMENT,
				this.adjustmentInterval);

		this.maximumBufferSize = GlobalConfiguration.getInteger(
				"channel.network.bufferSizeInBytes",
				GlobalBufferPool.DEFAULT_BUFFER_SIZE_IN_BYTES);
	}

	public long getAdjustmentInterval() {
		return this.adjustmentInterval;
	}

	HashSet<ChannelID> staleEdges = new HashSet<ChannelID>();

	public void adjustBufferSizes() throws InterruptedException {

		final HashMap<QosEdge, Integer> edgesToAdjust = new HashMap<QosEdge, Integer>();

		this.staleEdges.clear();

		QosConstraintViolationListener listener = new QosConstraintViolationListener() {
			@Override
			public void handleViolatedConstraint(
					List<QosGraphMember> sequenceMembers,
					double constraintViolatedByMillis) {
				if (constraintViolatedByMillis > 0) {
					BufferSizeManager.this.collectEdgesToAdjust(
							sequenceMembers, edgesToAdjust);
				}
			}
		};

		this.qosModel.findQosConstraintViolations(listener);

		this.doAdjust(edgesToAdjust);

		LOG.debug(String.format(
				"Adjusted edges: %d | Edges with stale Qos data: %d",
				edgesToAdjust.size(), this.staleEdges.size()));

		this.refreshTimeOfNextAdjustment();
	}

	private void doAdjust(HashMap<QosEdge, Integer> edgesToAdjust)
			throws InterruptedException {

		for (QosEdge edge : edgesToAdjust.keySet()) {
			int newBufferSize = edgesToAdjust.get(edge);

			BufferSizeHistory sizeHistory = edge.getQosData()
					.getBufferSizeHistory();
			sizeHistory.addToHistory(this.timeOfNextAdjustment, newBufferSize);

			this.setBufferSize(edge, newBufferSize);
		}
	}

	private void refreshTimeOfNextAdjustment() {
		long now = System.currentTimeMillis();
		while (this.timeOfNextAdjustment <= now) {
			this.timeOfNextAdjustment += this.adjustmentInterval;
		}
	}

	private void collectEdgesToAdjust(List<QosGraphMember> sequenceMembers,
			HashMap<QosEdge, Integer> edgesToAdjust) {

		for (QosGraphMember member : sequenceMembers) {

			if (member.isVertex()) {
				continue;
			}

			QosEdge edge = (QosEdge) member;

			EdgeQosData qosData = edge.getQosData();

			if (edgesToAdjust.containsKey(edge) || qosData.isInChain()) {
				continue;
			}

			if (!this.hasFreshValues(qosData)) {
				this.staleEdges.add(edge.getSourceChannelID());
				// LOG.info("Rejecting edge due to stale values: " +
				// QosUtils.formatName(edge));
				continue;
			}

			double outputBufferLatency = qosData
					.getOutputBufferLifetimeInMillis() / 2;
			double millisBetweenRecordEmissions = 1 / (qosData
					.getRecordsPerSecond() * 1000);

			if (outputBufferLatency > 5
					&& outputBufferLatency > millisBetweenRecordEmissions) {
				this.reduceBufferSize(qosData, edgesToAdjust);
			} else if (outputBufferLatency <= 1
					&& qosData.getBufferSize() < this.maximumBufferSize) {
				this.increaseBufferSize(qosData, edgesToAdjust);
			}
		}
	}

	private void increaseBufferSize(EdgeQosData qosData,
			HashMap<QosEdge, Integer> edgesToAdjust) {

		int oldBufferSize = qosData.getBufferSize();
		int newBufferSize = Math.min(
				this.proposeIncreasedBufferSize(oldBufferSize),
				this.maximumBufferSize);

		if (this.isRelevantIncrease(oldBufferSize, newBufferSize)
				|| newBufferSize == this.maximumBufferSize) {
			edgesToAdjust.put(qosData.getEdge(), newBufferSize);
		}
	}

	private boolean isRelevantIncrease(int oldBufferSize, int newBufferSize) {
		return newBufferSize >= oldBufferSize + 100;
	}

	private int proposeIncreasedBufferSize(int oldBufferSize) {
		return (int) (oldBufferSize * 1.2);
	}

	private void reduceBufferSize(EdgeQosData qosData,
			HashMap<QosEdge, Integer> edgesToAdjust) {

		int oldBufferSize = qosData.getBufferSizeHistory().getLastEntry()
				.getBufferSize();
		int newBufferSize = this.proposeReducedBufferSize(qosData,
				oldBufferSize);

		// filters pointless minor changes in buffer size
		if (this.isRelevantReduction(newBufferSize, oldBufferSize)) {
			edgesToAdjust.put(qosData.getEdge(), newBufferSize);
		}

		// else {
		// LOG.info(String.format("Filtering reduction due to insignificance: %s (old:%d new:%d)",
		// QosUtils.formatName(edge), oldBufferSize, newBufferSize));
		// }
	}

	private boolean isRelevantReduction(int newBufferSize, int oldBufferSize) {
		return newBufferSize < oldBufferSize * 0.98;
	}

	private int proposeReducedBufferSize(EdgeQosData qosData, int oldBufferSize) {

		double avgOutputBufferLatency = qosData
				.getOutputBufferLifetimeInMillis() / 2;

		double reductionFactor = Math.pow(0.95, avgOutputBufferLatency);
		reductionFactor = Math.max(0.01, reductionFactor);

		int newBufferSize = (int) Math.max(50, oldBufferSize * reductionFactor);

		return newBufferSize;
	}

	private boolean hasFreshValues(EdgeQosData qosData) {
		long freshnessThreshold = qosData.getBufferSizeHistory().getLastEntry()
				.getTimestamp();

		return qosData.isChannelLatencyFresherThan(freshnessThreshold)
				&& qosData
						.isOutputBufferLifetimeFresherThan(freshnessThreshold);
	}

	public boolean isAdjustmentNecessary(long now) {
		return now >= this.timeOfNextAdjustment;
	}

	private void setBufferSize(QosEdge edge, int bufferSize)
			throws InterruptedException {

		LimitBufferSizeAction limitBufferSizeAction = new LimitBufferSizeAction(
				this.jobID, edge.getOutputGate().getVertex().getID(), edge
						.getOutputGate().getGateID(),
				edge.getSourceChannelID(), bufferSize);

		InstanceConnectionInfo receiver = edge.getOutputGate().getVertex()
				.getExecutingInstance();
		this.messagingThread.sendToTaskManagerAsynchronously(receiver,
				limitBufferSizeAction);

	}
}
