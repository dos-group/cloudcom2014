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
package eu.stratosphere.nephele.streaming.taskmanager.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.streaming.util.SparseDelegateIterable;

/**
 * Tests on {@link SparseDelegateIterable}
 * 
 * @author Bernd Louis (bernd.louis@gmail.com)
 */
public class SparseDelegateIterableTest {
	private Collection<String> sparseCollection;

	@Before
	public void setUp() throws Exception {
		// Mutable vanilla ArrayList
		this.sparseCollection = new ArrayList<String>();
		this.sparseCollection.add(null);
		this.sparseCollection.add("one");
		this.sparseCollection.add(null);
		this.sparseCollection.add("two");
		this.sparseCollection.add(null);
		this.sparseCollection.add("three");
		this.sparseCollection.add(null);
	}

	@Test
	public void testNextBeforeHasNext() throws Exception {
		Iterator<String> iterator = this
				.createSparseIterator(this.sparseCollection);
		assertTrue(iterator.hasNext());
		assertEquals("one", iterator.next());
	}

	@Test
	public void testNextWithoutPriorHasNextShouldReturnFirstElement()
			throws Exception {
		assertEquals("one", this.createSparseIterator(this.sparseCollection)
				.next());
	}

	@Test
	public void testShouldAdvanceCurrentElement() throws Exception {
		Iterator<String> it = this.createSparseIterator(this.sparseCollection);
		it.hasNext();
		String shouldBeOne = it.next();
		assertEquals("one", shouldBeOne);
		String shouldBeTwo = it.next();
		assertEquals("Calling next twice should yield `two`", "two",
				shouldBeTwo);
	}

	/**
	 * Remove is currently not allowed on {@link SparseDelegateIterable}
	 * 
	 * @throws Exception
	 */
	@Test(expected = UnsupportedOperationException.class)
	public void testRemoveElementShouldFail() throws Exception {
		Iterator<String> it = this.createSparseIterator(this.sparseCollection);
		it.next();
		it.remove();
	}

	@Test
	public void testHasNextShouldNotAdvanceTheCurrentElement() throws Exception {
		Iterator<String> it = this.createSparseIterator(this.sparseCollection);
		it.next();
		it.hasNext();
		it.hasNext();
		assertEquals("two", it.next());
	}

	@Test(expected = NoSuchElementException.class)
	public void testNoSuchElementExceptionShouldBeThrownAtListEnd()
			throws Exception {
		Iterator<String> it = this.createSparseIterator(this.sparseCollection);
		try {
			it.next();
			it.next();
			it.next();
		} catch (NoSuchElementException e) {
			fail("NoSuchElementException should not be thrown on the first three next() calls");
		}
		assertFalse(it.hasNext());
		it.next(); // should throw NoSuchElementException
	}

	/**
	 * Helper creates a new sparse Iterator from a generic {@link Iterable}
	 * 
	 * @param iterable
	 *            a generic {@link Iterable}
	 * @param <T>
	 *            Implied type argument, may be omitted
	 * @return a {@link SparseDelegateIterable}
	 */
	private <T> Iterator<T> createSparseIterator(Iterable<T> iterable) {
		return new SparseDelegateIterable<T>(iterable.iterator()).iterator();
	}
}
