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

package eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamOutputGate;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public final class RuntimeChain {

	private ArrayList<RuntimeChainLink> chainLinks = new ArrayList<RuntimeChainLink>();

	private final AtomicBoolean tasksSuccessfullyChained = new AtomicBoolean(
			false);

	public RuntimeChain(List<RuntimeChainLink> chainLinks) {

		if (chainLinks.size() < 2) {
			throw new IllegalArgumentException(
					"At least 2 chain links are required!");
		}

		this.chainLinks.addAll(chainLinks);
	}

	public void writeRecord(final Record record) throws IOException {
		try {
			this.executeMappers(record, 1);
		} catch (Exception e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void executeMappers(Record record, int indexInChain)
			throws Exception {
		// TODO this will be much nicer once we have an IOC interface to
		// write Nephele tasks

		RuntimeChainLink chainLink = this.chainLinks.get(indexInChain);
		Mapper mapper = chainLink.getMapper();
		StreamOutputGate outputGate = chainLink.getOutputGate();

		chainLink.getInputGate().reportRecordReceived(record, 0);
		mapper.map(record);
		Queue outputCollector = mapper.getOutputCollector();

		boolean isLastInChain = indexInChain == this.chainLinks.size() - 1;
		if (isLastInChain) {
			while (!outputCollector.isEmpty()) {
				outputGate.writeRecord((Record) outputCollector.poll());
			}
		} else {
			while (!outputCollector.isEmpty()) {
				Record outputRecord = (Record) outputCollector.poll();
				outputGate.reportRecordEmitted(outputRecord, 0);
				this.executeMappers(RecordUtils.createCopy(outputRecord),
						indexInChain + 1);
			}
		}
	}

	public List<RuntimeChainLink> getChainLinks() {
		return this.chainLinks;
	}

	public void waitUntilTasksAreChained() throws InterruptedException {
		synchronized (this.tasksSuccessfullyChained) {
			if (!this.tasksSuccessfullyChained.get()) {
				this.tasksSuccessfullyChained.wait();
			}
		}
	}

	public void signalTasksAreSuccessfullyChained() {
		synchronized (this.tasksSuccessfullyChained) {
			this.tasksSuccessfullyChained.set(true);
			this.tasksSuccessfullyChained.notify();
		}
	}

	public StreamOutputGate<? extends Record> getFirstOutputGate() {
		return this.chainLinks.get(0).getOutputGate();
	}

	public StreamInputGate<? extends Record> getFirstInputGate() {
		return this.chainLinks.get(0).getInputGate();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.chainLinks.get(0).toString());
		
		for(int i=1; i< this.chainLinks.size(); i++) {
			builder.append("->");
			builder.append(this.chainLinks.get(i).toString());
		}
		
		return builder.toString();
	}
}
