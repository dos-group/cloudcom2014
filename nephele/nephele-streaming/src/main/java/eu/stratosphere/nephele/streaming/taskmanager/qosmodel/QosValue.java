package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

public class QosValue implements Comparable<QosValue> {

	private static long nextFreeId = 0;

	private long id;

	private double value;

	private long timestamp;

	public QosValue(double value, long timestamp) {
		this.value = value;
		this.timestamp = timestamp;
		this.id = nextFreeId++;
	}

	public double getValue() {
		return this.value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getId() {
		return this.id;
	}

	/**
	 * Sorts first by value and then by id.
	 */
	@Override
	public int compareTo(QosValue other) {
		if (this.value > other.value) {
			return 1;
		} else if (this.value < other.value) {
			return -1;
		} else {
			if (this.id > other.id) {
				return 1;
			} else if (this.id < other.id) {
				return -1;
			} else {
				return 0;
			}
		}
	}

	@Override
	public boolean equals(Object otherObj) {
		if (otherObj instanceof QosValue) {
			QosValue other = (QosValue) otherObj;
			return other.id == this.id;
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return Long.valueOf(this.id).hashCode();
	}

}
