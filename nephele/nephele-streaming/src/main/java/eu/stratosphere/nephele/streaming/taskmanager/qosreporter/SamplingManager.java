package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.util.concurrent.ThreadLocalRandom;

public class SamplingManager {

	private boolean firstSample = true;
	private double samplingProbability;


	public SamplingManager(double samplingProbability) {
		this.samplingProbability = samplingProbability;
	}

	public boolean shouldSample() {
		if (firstSample) {
			firstSample = false;
			return true;
		}
		return ThreadLocalRandom.current().nextDouble() < samplingProbability;
	}

	public void reset() {
		firstSample = true;
	}
}
