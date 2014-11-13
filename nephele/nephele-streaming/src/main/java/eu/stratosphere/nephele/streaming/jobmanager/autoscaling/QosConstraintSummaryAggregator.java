package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintViolationReport;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosManagerID;

public class QosConstraintSummaryAggregator {
	
	private final HashMap<QosManagerID, QosConstraintSummary> summaries = new HashMap<QosManagerID, QosConstraintSummary>();
	
	private final Set<QosManagerID> requiredManagerIDs;

	private final JobGraphLatencyConstraint constraint; 
	
	public QosConstraintSummaryAggregator(JobGraphLatencyConstraint constraint,
			Set<QosManagerID> managerIDs) {

		this.constraint = constraint;
		this.requiredManagerIDs = managerIDs;
	}
	
	public JobGraphLatencyConstraint getConstraint() {
		return constraint;
	}

	public void add(QosManagerID qosManagerID, QosConstraintSummary summary) {
		summaries.put(qosManagerID, summary);
	}
	
	public boolean canAggregate() {
		return this.summaries.size() == this.requiredManagerIDs.size();
	}
	
	public QosConstraintSummary computeAggregation() {
		QosConstraintViolationReport vioRep = new QosConstraintViolationReport(constraint);
		QosConstraintSummary aggregation = new QosConstraintSummary(constraint, vioRep);

		List<QosConstraintSummary> completeSummaries = getCompleteSummaries();
		aggregation.merge(completeSummaries);
		return aggregation;
	}

	private List<QosConstraintSummary> getCompleteSummaries() {
		List<QosConstraintSummary> ret = new LinkedList<QosConstraintSummary>();
		for (QosConstraintSummary summary : summaries.values()) {
			if (summary.hasData()) {
				ret.add(summary);
			}
		}
		return ret;
	}
}
