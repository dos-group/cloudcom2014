package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class QosStatisticTest {

	private QosStatistic qosStatistic;

	@Before
	public void setup() {
		this.qosStatistic = new QosStatistic(7);
	}

	@Test
	public void testValueSorted() {
		this.qosStatistic.addValue(this.createQosValue(1, 1));
		this.qosStatistic.addValue(this.createQosValue(2, 3));
		this.qosStatistic.addValue(this.createQosValue(3, 7));
		this.qosStatistic.addValue(this.createQosValue(4, 8));
		this.qosStatistic.addValue(this.createQosValue(5, 21));
		this.qosStatistic.addValue(this.createQosValue(6, 35));
		this.qosStatistic.addValue(this.createQosValue(7, 41));

		assertTrue(this.qosStatistic.getMedianValue() == 8);
		assertTrue(this.qosStatistic.getMinValue() == 1);
		assertTrue(this.qosStatistic.getMaxValue() == 41);
		assertTrue(this.qosStatistic.getArithmeticMean() - 16.5714 < 0.0001);
	}

	private QosValue createQosValue(long timestamp, double value) {
		return new QosValue(value, timestamp);
	}

	@Test
	public void testAddValueUnsorted() {
		this.qosStatistic.addValue(this.createQosValue(1, 7));
		this.qosStatistic.addValue(this.createQosValue(2, 15));
		this.qosStatistic.addValue(this.createQosValue(3, 13));
		this.qosStatistic.addValue(this.createQosValue(4, 1));
		this.qosStatistic.addValue(this.createQosValue(5, 5));
		this.qosStatistic.addValue(this.createQosValue(6, 7.5));
		this.qosStatistic.addValue(this.createQosValue(7, 8));

		assertTrue(this.qosStatistic.getMedianValue() == 7.5);
		assertTrue(this.qosStatistic.getMinValue() == 1);
		assertTrue(this.qosStatistic.getMaxValue() == 15);
		assertTrue(this.qosStatistic.getArithmeticMean() - 8.0714 < 0.0001);
	}

	@Test
	public void testAddValueReverseSorted() {
		this.qosStatistic.addValue(this.createQosValue(1, 18));
		this.qosStatistic.addValue(this.createQosValue(2, 15));
		this.qosStatistic.addValue(this.createQosValue(3, 13));
		this.qosStatistic.addValue(this.createQosValue(4, 10));
		this.qosStatistic.addValue(this.createQosValue(5, 9));
		this.qosStatistic.addValue(this.createQosValue(6, 8));
		this.qosStatistic.addValue(this.createQosValue(7, 7));

		assertTrue(this.qosStatistic.getMedianValue() == 10);
		assertTrue(this.qosStatistic.getMinValue() == 7);
		assertTrue(this.qosStatistic.getMaxValue() == 18);
		assertTrue(this.qosStatistic.getArithmeticMean() - 11.4285 < 0.0001);
	}

	@Test
	public void testAddValueOverfullUnsorted() {
		this.qosStatistic.addValue(this.createQosValue(1, 7));
		this.qosStatistic.addValue(this.createQosValue(2, 15));
		this.qosStatistic.addValue(this.createQosValue(3, 13));
		this.qosStatistic.addValue(this.createQosValue(4, 1));
		this.qosStatistic.addValue(this.createQosValue(5, 7.5));
		this.qosStatistic.addValue(this.createQosValue(6, 5));
		this.qosStatistic.addValue(this.createQosValue(7, 18));
		this.qosStatistic.addValue(this.createQosValue(8, 13));
		this.qosStatistic.addValue(this.createQosValue(9, 10));
		this.qosStatistic.addValue(this.createQosValue(10, 8));

		assertTrue(this.qosStatistic.getMedianValue() == 8);
		assertTrue(this.qosStatistic.getMinValue() == 1);
		assertTrue(this.qosStatistic.getMaxValue() == 18);
		assertTrue(this.qosStatistic.getArithmeticMean() - 8.9285 < 0.0001);
	}

	@Test
	public void testAddValueOverfullSorted() {
		this.qosStatistic.addValue(this.createQosValue(1, 1));
		this.qosStatistic.addValue(this.createQosValue(2, 2));
		this.qosStatistic.addValue(this.createQosValue(3, 3));
		this.qosStatistic.addValue(this.createQosValue(4, 4));
		this.qosStatistic.addValue(this.createQosValue(5, 5));
		this.qosStatistic.addValue(this.createQosValue(6, 6));
		this.qosStatistic.addValue(this.createQosValue(7, 7));
		this.qosStatistic.addValue(this.createQosValue(8, 8));
		this.qosStatistic.addValue(this.createQosValue(9, 9));
		this.qosStatistic.addValue(this.createQosValue(10, 10));

		assertTrue(this.qosStatistic.getMedianValue() == 7);
		assertTrue(this.qosStatistic.getMinValue() == 4);
		assertTrue(this.qosStatistic.getMaxValue() == 10);
		assertTrue(this.qosStatistic.getArithmeticMean() == 7);
	}

	@Test
	public void testGetMedianUnderfull() {
		this.qosStatistic.addValue(this.createQosValue(1, 18));
		this.qosStatistic.addValue(this.createQosValue(2, 15));
		assertTrue(this.qosStatistic.getMedianValue() == 18);

		this.qosStatistic.addValue(this.createQosValue(3, 17));
		assertTrue(this.qosStatistic.getMedianValue() == 17);
	}

	@Test
	public void testGetMinMaxUnderfull() {
		this.qosStatistic.addValue(this.createQosValue(1, 18));
		this.qosStatistic.addValue(this.createQosValue(2, 15));
		assertTrue(this.qosStatistic.getMinValue() == 15);
		assertTrue(this.qosStatistic.getMaxValue() == 18);

		this.qosStatistic.addValue(this.createQosValue(3, 17));
		assertTrue(this.qosStatistic.getMinValue() == 15);
		assertTrue(this.qosStatistic.getMaxValue() == 18);

	}

	@Test
	public void testGetArithmeticMeanUnderfull() {
		this.qosStatistic.addValue(this.createQosValue(1, 18));
		this.qosStatistic.addValue(this.createQosValue(2, 15));
		assertTrue(this.qosStatistic.getArithmeticMean() == 16.5);

		this.qosStatistic.addValue(this.createQosValue(3, 17));
		assertTrue(this.qosStatistic.getArithmeticMean() - 16.6666 < 0.0001);
	}
}
