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

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkOutputChannel;
import eu.stratosphere.nephele.plugins.wrapper.AbstractOutputGateWrapper;
import eu.stratosphere.nephele.streaming.message.action.DropCurrentChainAction;
import eu.stratosphere.nephele.streaming.message.action.EstablishNewChainAction;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.message.action.QosAction;
import eu.stratosphere.nephele.streaming.message.action.SetOutputLatencyTargetAction;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener.OutputGateQosReportingListener;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.RuntimeChain;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.RuntimeChainLink;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

/**
 * Wraps Nephele's {@link eu.stratosphere.nephele.io.RuntimeOutputGate} to
 * intercept methods calls necessary for Qos statistics collection.
 * 
 * @author Bjoern Lohrmann
 * 
 * @param <T>
 */
public final class StreamOutputGate<T extends Record> extends
		AbstractOutputGateWrapper<T> {

	private final static Logger LOG = Logger.getLogger(StreamOutputGate.class);

	private RuntimeChain streamChain = null;

	private volatile OutputGateQosReportingListener qosCallback;

	private HashMap<ChannelID, AbstractOutputChannel<T>> outputChannels;

	private StreamChannelSelector<T> streamChannelSelector;

	private LinkedBlockingQueue<QosAction> qosActionQueue;

	public StreamOutputGate(final OutputGate<T> wrappedOutputGate,
			StreamChannelSelector<T> streamChannelSelector) {
		super(wrappedOutputGate);
		this.outputChannels = new HashMap<ChannelID, AbstractOutputChannel<T>>();
		this.streamChannelSelector = streamChannelSelector;
		this.qosActionQueue = new LinkedBlockingQueue<QosAction>();
	}

	public void setQosReportingListener(
			OutputGateQosReportingListener qosCallback) {
		this.qosCallback = qosCallback;
	}

	public OutputGateQosReportingListener getQosReportingListener() {
		return this.qosCallback;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(final T record) throws IOException,
			InterruptedException {
		
		int outputChannel = this.streamChannelSelector.invokeWrappedChannelSelector(record,
						this.getNumberOfActiveOutputChannels())[0];
		
		this.reportRecordEmitted(record, outputChannel);

		if (this.streamChain == null) {
			this.getWrappedOutputGate().writeRecord(record);
		} else {
			this.streamChain.writeRecord(record);
		}

		this.handlePendingQosActions();
	}

	public void enqueueQosAction(QosAction qosAction) {
		this.qosActionQueue.add(qosAction);
	}

	private void handlePendingQosActions() throws InterruptedException,
			IOException {
		QosAction action;
		while ((action = this.qosActionQueue.poll()) != null) {
			if (action instanceof LimitBufferSizeAction) {
				this.limitBufferSize((LimitBufferSizeAction) action);
			} else if (action instanceof SetOutputLatencyTargetAction) {
				this.setOutputBufferLatencyTarget((SetOutputLatencyTargetAction) action);
			} else if (action instanceof EstablishNewChainAction) {
				this.establishChain((EstablishNewChainAction) action);
			} else if (action instanceof DropCurrentChainAction) {
				reportChainState(false);
				dropCurrentChain();
			}
		}
	}

	private void setOutputBufferLatencyTarget(SetOutputLatencyTargetAction action) {
		ChannelID channelID = action.getSourceChannelID();

		AbstractByteBufferedOutputChannel<T> channel = (AbstractByteBufferedOutputChannel<T>) this.outputChannels
				.get(channelID);

		if (channel == null) {
			LOG.error("Cannot find output channel with ID " + channelID);
			return;
		}
		
		channel.setAutoflushInterval(action.getOutputBufferLatencyTarget() * 2);		
	}

	private void dropCurrentChain() {
		LOG.info("Dropped chain " + this.streamChain);
		this.streamChain = null;
	}

	public AbstractOutputChannel<T> getOutputChannel(ChannelID channelID) {
		return this.outputChannels.get(channelID);
	}

	private void establishChain(EstablishNewChainAction chainTasksAction)
			throws InterruptedException, IOException {

		RuntimeChain streamChain = chainTasksAction.getRuntimeChain();

		if (getGateState() == GateState.RUNNING) {
			this.streamChain = streamChain;
			this.flush();
			reportChainState(true); // flush does not take the chain => report new chain after flush

			for (RuntimeChainLink chainLink : streamChain.getChainLinks()
					.subList(1, streamChain.getChainLinks().size() - 1)) {

				chainLink.getInputGate().haltTaskThreadIfNecessary();
				chainLink.getInputGate().reportChainState(true);
				chainLink.getOutputGate().flush();
				chainLink.getOutputGate().streamChain = null;
				chainLink.getOutputGate().reportChainState(true);
			}

			// special link: last output gate is not really part of this chain
			RuntimeChainLink lastChainLink =
					streamChain.getChainLinks().get(streamChain.getChainLinks().size() - 1);
			lastChainLink.getInputGate().haltTaskThreadIfNecessary();
			lastChainLink.getInputGate().reportChainState(true);
			lastChainLink.getOutputGate().streamChain = null;
			lastChainLink.getOutputGate().reportChainState(false);

			streamChain.signalTasksAreSuccessfullyChained();
			LOG.info("Established chain " + streamChain);

		} else {
			streamChain.signalTasksAreSuccessfullyChained();
			LOG.info("Ignoring chain request on chain " + streamChain + " (gate is not in running state!).");
		}
	}

	private void limitBufferSize(LimitBufferSizeAction lbsa) {
		// do nothing
	}

	public void reportRecordEmitted(final T record, int outputChannel) {
		if (this.qosCallback != null) {
			AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
			this.qosCallback.recordEmitted(outputChannel, taggableRecord);
		}
	}

	public void reportChainState(boolean isChained) {
		if (this.qosCallback != null)
			this.qosCallback.setChained(isChained);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void outputBufferSent(final int channelIndex) {		
		if (this.qosCallback != null) {
			this.qosCallback.outputBufferSent(channelIndex, this
					.getOutputChannel(channelIndex)
					.getAmountOfDataTransmitted());
		}
		this.getWrappedOutputGate().outputBufferSent(channelIndex);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkOutputChannel<T> createNetworkOutputChannel(
			final OutputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID) {

		NetworkOutputChannel<T> channel = this.getWrappedOutputGate()
				.createNetworkOutputChannel(inputGate, channelID,
						connectedChannelID);

		this.outputChannels.put(channelID, channel);

		return channel;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InMemoryOutputChannel<T> createInMemoryOutputChannel(
			final OutputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID) {

		InMemoryOutputChannel<T> channel = this.getWrappedOutputGate()
				.createInMemoryOutputChannel(inputGate, channelID,
						connectedChannelID);

		this.outputChannels.put(channelID, channel);
		return channel;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelSelector<T> getChannelSelector() {
		return this.streamChannelSelector;
	}

	private void dropCurrentChainAndWakeUpChainedTasks() {
		RuntimeChain oldChain = this.streamChain;

		if (oldChain != null) {
			dropCurrentChain();
			reportChainState(false);

			for (RuntimeChainLink chainLink : oldChain.getChainLinks().subList(
					1, oldChain.getChainLinks().size())) {

				chainLink.getOutputGate().dropCurrentChain();
				chainLink.getOutputGate().reportChainState(false);
				chainLink.getInputGate().reportChainState(false);
				chainLink.getInputGate().wakeUpTaskThreadIfNecessary();
			}
		}
	}

	@Override
	public void requestSuspend() throws IOException, InterruptedException {
		super.requestSuspend(); // put gate in drain mode and send suspend event
		dropCurrentChainAndWakeUpChainedTasks();
	}

	@Override
	public void requestClose() throws IOException, InterruptedException {
		super.requestClose();
		dropCurrentChainAndWakeUpChainedTasks();
	}
}
