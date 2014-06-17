package eu.stratosphere.nephele.streaming.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * This message, sent by the Qos reporters on a task manager to their common QoS
 * manager, announces the (un)chaining of edges. The QoS manager can then update
 * its internal model accordingly.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class ChainUpdates extends AbstractSerializableQosMessage {

	private LinkedList<QosReporterID.Edge> unchainedEdges = new LinkedList<QosReporterID.Edge>();

	private LinkedList<QosReporterID.Edge> newlyChainedEdges = new LinkedList<QosReporterID.Edge>();

	public ChainUpdates(JobID jobID) {
		super(jobID);
	}
	
	public ChainUpdates() {
	}

	public List<QosReporterID.Edge> getUnchainedEdges() {
		return this.unchainedEdges;
	}

	public List<QosReporterID.Edge> getNewlyChainedEdges() {
		return this.newlyChainedEdges;
	}

	public void addUnchainedEdge(QosReporterID.Edge unchainedEdge) {
		this.unchainedEdges.add(unchainedEdge);
	}

	public void addNewlyChainedEdge(QosReporterID.Edge newlyChainedEdge) {
		this.newlyChainedEdges.add(newlyChainedEdge);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		
		out.writeInt(this.unchainedEdges.size());
		for (QosReporterID.Edge unchainedEdge : this.unchainedEdges) {
			unchainedEdge.write(out);
		}
		out.writeInt(this.newlyChainedEdges.size());
		for (QosReporterID.Edge newlyChainedEdge : this.newlyChainedEdges) {
			newlyChainedEdge.write(out);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		int noOfUnchainedEdges = in.readInt();
		for (int i = 0; i < noOfUnchainedEdges; i++) {
			QosReporterID.Edge unchainedEdge = new QosReporterID.Edge();
			unchainedEdge.read(in);
			this.unchainedEdges.add(unchainedEdge);
		}

		int noOfNewlyChainedEdges = in.readInt();
		for (int i = 0; i < noOfNewlyChainedEdges; i++) {
			QosReporterID.Edge newlyChainedEdge = new QosReporterID.Edge();
			newlyChainedEdge.read(in);
			this.newlyChainedEdges.add(newlyChainedEdge);
		}
	}
}
