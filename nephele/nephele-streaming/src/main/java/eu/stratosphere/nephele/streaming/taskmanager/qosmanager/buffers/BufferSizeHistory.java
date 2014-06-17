package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

/**
 * Stores a time-series of output buffer sizes (represented as ints). Instances
 * of this class are usually associated by the Qos data of a Qos edge.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class BufferSizeHistory {

	private BufferSizeHistoryEntry[] entries;

	private int entriesInHistory;

	public BufferSizeHistory(int noOfHistoryEntries) {
		this.entries = new BufferSizeHistoryEntry[noOfHistoryEntries];
		this.entriesInHistory = 0;
	}

	public void addToHistory(long timestamp, int newBufferSize) {
		BufferSizeHistoryEntry newEntry = new BufferSizeHistoryEntry(Math.min(
				this.entriesInHistory, this.entries.length - 1), timestamp,
				newBufferSize);

		if (this.entriesInHistory < this.entries.length) {
			this.entries[this.entriesInHistory] = newEntry;
			this.entriesInHistory++;
		} else {
			System.arraycopy(this.entries, 1, this.entries, 0,
					this.entriesInHistory - 1);
			this.entries[this.entriesInHistory - 1] = newEntry;
		}
	}

	public BufferSizeHistoryEntry[] getEntries() {
		return this.entries;
	}

	public BufferSizeHistoryEntry getFirstEntry() {
		return this.entries[0];
	}

	public BufferSizeHistoryEntry getLastEntry() {
		if (this.entriesInHistory > 0) {
			return this.entries[this.entriesInHistory - 1];
		}

		return null;
	}

	public boolean hasEntries() {
		return this.entriesInHistory > 0;
	}

	public int getNumberOfEntries() {
		return this.entriesInHistory;
	}
}
