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
package eu.stratosphere.nephele.streaming.util;

import java.util.Iterator;

/**
 * Provides convenience methods that wrap the sometimes bulky constructor calls
 * to other classes in this package.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class StreamUtil {

	public static <V> SparseDelegateIterable<V> toIterable(
			Iterator<V> sparseIterator) {
		return new SparseDelegateIterable<V>(sparseIterator);
	}
}
