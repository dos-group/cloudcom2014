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

package eu.stratosphere.nephele.streaming.message.action;

import eu.stratosphere.nephele.streaming.message.AbstractQosMessage;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.RuntimeChain;

/**
 * This class is used to signal to a StreamOutputGate that it is supposed to
 * establish a chain (series of adjacent tasks with pointwise wiring pattern
 * in-between that are executed within the same thread).
 * 
 * @author Bjoern Lohrmann
 */
public final class EstablishNewChainAction extends AbstractQosMessage implements QosAction {

	private final RuntimeChain runtimeChain;

	public EstablishNewChainAction(RuntimeChain runtimeChain) {
		this.runtimeChain = runtimeChain;
	}

	/**
	 * Returns the runtime chain.
	 * 
	 * @return the runtime chain.
	 */
	public RuntimeChain getRuntimeChain() {
		return this.runtimeChain;
	}
}
