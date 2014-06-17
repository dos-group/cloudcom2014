/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.nephele.streaming.taskmanager.runtime.io;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Implements a round robin channel schedule. All operations have O(1)
 * complexity.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class RoundRobinChannelSchedule {

	/**
	 * Used to efficiently detect whether a given channel is already in the
	 * schedule (without going through the schedule).
	 */
	private HashSet<Integer> scheduledChannels;

	/**
	 * Holds the channels in the round robin schedule (without duplicates).
	 * channelSchedule[first] holds the next channel in the schedule and
	 * channelSchedule[last] holds the last one. A wraparound (last < first) is
	 * allowed.
	 */
	private int[] channelSchedule;

	/**
	 * Index of first channel in channelSchedule.
	 */
	private int first;

	/**
	 * Index of last channel in channelSchedule.
	 */
	private int last;

	/**
	 * Number of channels in channel schedule.
	 */
	private int channelCount;

	public RoundRobinChannelSchedule(int nofOfChannels) {
		this.scheduledChannels = new HashSet<Integer>(nofOfChannels);
		this.channelSchedule = new int[nofOfChannels];
		Arrays.fill(this.channelSchedule, -1);
		this.first = 0;
		this.last = -1;
		this.channelCount = 0;
	}

	public RoundRobinChannelSchedule() {
		this(10);
	}

	/**
	 * @return the next channel in the round robin schedule, or -1 if the
	 *         schedule is empty.
	 */
	public int nextChannel() {
		int channel = -1;
		if (this.channelCount > 0) {
			channel = this.removeFirst();
			this.addLast(channel);
		}

		return channel;
	}

	public void unscheduleCurrentChannel() {
		if (this.channelCount == 0) {
			return;
		}

		this.channelCount--;
		int dropped = this.channelSchedule[this.last];
		this.channelSchedule[this.last] = -1;
		if (this.last > 0) {
			this.last--;
		} else {
			this.last = this.channelSchedule.length - 1;
		}
		this.scheduledChannels.remove(dropped);

	}

	public void scheduleChannel(int channelIndex) {
		boolean added = this.scheduledChannels.add(channelIndex);
		if (added) {
			this.resizeChannelArrayIfFull();
			this.channelCount++;
			this.addLast(channelIndex);
		}
	}

	private void resizeChannelArrayIfFull() {
		if (this.channelCount >= this.channelSchedule.length) {
			int[] newSchedule = new int[this.channelSchedule.length * 2];
			Arrays.fill(newSchedule, -1);

			// begin by copying everything after (including) first into
			// newSchedule
			int toCopy = this.channelSchedule.length - this.first;
			int newFirst = newSchedule.length - toCopy;
			int newLast = newSchedule.length - 1;
			System.arraycopy(this.channelSchedule, this.first, newSchedule,
					newFirst, toCopy);

			// in case of wrap-around indices, also copy everything before
			// (including) last into newSchedule
			if (this.last < this.first) {
				System.arraycopy(this.channelSchedule, 0, newSchedule, 0,
						this.last + 1);
				newLast = this.last;
			}
			this.channelSchedule = newSchedule;
			this.first = newFirst;
			this.last = newLast;
		}
	}

	private int removeFirst() {
		int removed = this.channelSchedule[this.first];
		this.channelSchedule[this.first] = -1;
		this.first = (this.first + 1) % this.channelSchedule.length;
		return removed;
	}

	private void addLast(int channel) {
		this.last = (this.last + 1) % this.channelSchedule.length;
		this.channelSchedule[this.last] = channel;
	}

	public boolean isEmpty() {
		return this.channelCount == 0;
	}

}
