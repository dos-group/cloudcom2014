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

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.types.Record;

/**
 * Wraps the actual channel selector of a
 * {@link eu.stratosphere.nephele.io.RuntimeOutputGate}. This is necessary
 * because the {@link StreamOutputGate} needs to invoke
 * {@link #selectChannels(Record, int)} once per record before handing the
 * record to Nephele's own {@link eu.stratosphere.nephele.io.RuntimeOutputGate},
 * which will invoke this method again. Some channel selectors (especially
 * Nephele's default channel selector) react badly to this, because they expect
 * EXACTLY ONE invocation of {@link #selectChannels(Record, int)} per record.
 * Therefore, this wrapper makes sure, that only one invocation occurs and
 * caches the result.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class StreamChannelSelector<T extends Record> implements
		ChannelSelector<T> {

	private ChannelSelector<T> wrapped;
	private int[] nextChannelToSendTo;

	public StreamChannelSelector(ChannelSelector<T> toWrap) {
		this.wrapped = toWrap;
	}

	public int[] invokeWrappedChannelSelector(final T record,
			final int numberOfOutputChannels) {
		this.nextChannelToSendTo = this.wrapped.selectChannels(record,
				numberOfOutputChannels);
		return this.nextChannelToSendTo;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.ChannelSelector#selectChannels(eu.stratosphere
	 * .nephele.types.Record, int)
	 */
	@Override
	public int[] selectChannels(T record, int numberOfOutputChannels) {
		return this.nextChannelToSendTo;
	}
}
