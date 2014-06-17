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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class RoundRobinChannelScheduleTest {

	private RoundRobinChannelSchedule schedule;

	@Before
	public void setup() {
		this.schedule = new RoundRobinChannelSchedule(4);
	}

	@Test
	public void testNoChannels() {
		assertEquals(-1, this.schedule.nextChannel());
	}

	@Test
	public void testOneChannel() {
		this.schedule.scheduleChannel(7);
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(7, this.schedule.nextChannel());
		this.schedule.unscheduleCurrentChannel();
		assertEquals(-1, this.schedule.nextChannel());
	}

	@Test
	public void testMultipleChannels() {
		this.schedule.scheduleChannel(7);
		this.schedule.scheduleChannel(1);
		this.schedule.scheduleChannel(8);
		this.schedule.scheduleChannel(2);
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(1, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		assertEquals(2, this.schedule.nextChannel());
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(1, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		assertEquals(2, this.schedule.nextChannel());
		this.schedule.unscheduleCurrentChannel();

		assertEquals(7, this.schedule.nextChannel());
		assertEquals(1, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(1, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		assertEquals(7, this.schedule.nextChannel());
		this.schedule.unscheduleCurrentChannel();

		assertEquals(1, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		assertEquals(1, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		this.schedule.unscheduleCurrentChannel();

		assertEquals(1, this.schedule.nextChannel());
		assertEquals(1, this.schedule.nextChannel());
		this.schedule.unscheduleCurrentChannel();

		assertEquals(-1, this.schedule.nextChannel());
	}

	@Test
	public void testDynamicAddRemove() {
		this.schedule.scheduleChannel(7);
		this.schedule.scheduleChannel(1);
		this.schedule.scheduleChannel(8);

		assertEquals(7, this.schedule.nextChannel());

		this.schedule.unscheduleCurrentChannel();

		assertEquals(1, this.schedule.nextChannel());

		this.schedule.scheduleChannel(10);

		assertEquals(8, this.schedule.nextChannel());
		assertEquals(1, this.schedule.nextChannel());
		assertEquals(10, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		this.schedule.scheduleChannel(47);

		assertEquals(1, this.schedule.nextChannel());
		assertEquals(10, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		assertEquals(47, this.schedule.nextChannel());

		this.schedule.unscheduleCurrentChannel();
		assertEquals(1, this.schedule.nextChannel());
		this.schedule.unscheduleCurrentChannel();
		assertEquals(10, this.schedule.nextChannel());
		this.schedule.unscheduleCurrentChannel();
		assertEquals(8, this.schedule.nextChannel());
		this.schedule.unscheduleCurrentChannel();
		assertEquals(-1, this.schedule.nextChannel());
	}

	@Test
	public void testScheduleRunsEmptyAndIsRefilled() {
		this.schedule.scheduleChannel(7);
		this.schedule.unscheduleCurrentChannel();

		assertEquals(-1, this.schedule.nextChannel());
		this.schedule.scheduleChannel(3);
		this.schedule.scheduleChannel(4);
		this.schedule.scheduleChannel(5);
		assertEquals(3, this.schedule.nextChannel());
		assertEquals(4, this.schedule.nextChannel());
		assertEquals(5, this.schedule.nextChannel());
		this.schedule.unscheduleCurrentChannel();
		this.schedule.unscheduleCurrentChannel();
		this.schedule.unscheduleCurrentChannel();
		assertEquals(-1, this.schedule.nextChannel());
	}

	@Test
	public void testInternalResize() {
		this.schedule.scheduleChannel(7);
		this.schedule.scheduleChannel(8);
		this.schedule.scheduleChannel(9);
		this.schedule.scheduleChannel(10);
		this.schedule.scheduleChannel(11);
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		assertEquals(9, this.schedule.nextChannel());
		assertEquals(10, this.schedule.nextChannel());
		assertEquals(11, this.schedule.nextChannel());
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		assertEquals(9, this.schedule.nextChannel());
		assertEquals(10, this.schedule.nextChannel());
		assertEquals(11, this.schedule.nextChannel());
	}

	@Test
	public void testInternalResizeWithWraparound() {
		this.schedule.scheduleChannel(7);
		this.schedule.scheduleChannel(8);
		this.schedule.scheduleChannel(9);
		this.schedule.scheduleChannel(10);
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());

		this.schedule.scheduleChannel(11);

		assertEquals(9, this.schedule.nextChannel());
		assertEquals(10, this.schedule.nextChannel());
		assertEquals(7, this.schedule.nextChannel());
		assertEquals(8, this.schedule.nextChannel());
		assertEquals(11, this.schedule.nextChannel());
		assertEquals(9, this.schedule.nextChannel());
		assertEquals(10, this.schedule.nextChannel());
	}

}
