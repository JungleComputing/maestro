package ibis.maestro;

/**
 * This class maintains a time estimate based on collected samples.
 * 
 * @author Kees van Reeuwijk.
 */
class PreciseDecayingTimeEstimate {
	private static final int BUFFER_SIZE = 40;
	private int sampleCount = 0;
	private long samples[] = new long[BUFFER_SIZE];
	private long sampleTimes[] = new long[BUFFER_SIZE];
	private int nextSampleIndex = 0;
	private long decayInterval = Long.MAX_VALUE;
	private int firstSample = 0;

	PreciseDecayingTimeEstimate(long initial) {
		samples[0] = initial;
		sampleTimes[0] = 0; // A long time ago.
		nextSampleIndex++;
	}

	/**
	 * Returns a string representation of this estimate.
	 */
	@Override
	public String toString() {
		return "average=" + Utils.formatNanoseconds(getAverage())
				+ " (based on " + sampleCount + " samples)";
	}

	/**
	 * Returns a reasonable average time estimate.
	 * 
	 * @return The average time.
	 */
	long getAverage() {
		long now = System.nanoTime();
		double sum = 0L;
		double weight = 1;
		int ix = nextSampleIndex;
		double totalWeight = 0;
		long interval = decayInterval;

		do {
			ix--;
			if (ix < 0) {
				ix = BUFFER_SIZE - 1;
			}
			while ((now - sampleTimes[ix]) > interval) {
				// We crossed a weight treshold, decrease the
				// weight of this and earlier samples.
				interval += interval;
				weight *= 0.5;
			}
			sum += samples[ix] / weight;
			totalWeight += 1 / weight;
		} while (ix != firstSample);
		long average = Math.round(sum / totalWeight);
		if (sampleCount > 4) {
			decayInterval = 5 * average;
		}
		return average;
	}

	/**
	 * Adds a new sample average to the estimate.
	 * 
	 * @param val
	 *            The new sample average to add.
	 */
	void addSample(long val) {
		long now = System.nanoTime();

		samples[nextSampleIndex] = val;
		sampleTimes[nextSampleIndex] = now;
		if (firstSample == nextSampleIndex) {
			firstSample++;
			if (firstSample >= BUFFER_SIZE) {
				firstSample = 0;
			}
		}
		nextSampleIndex++;
		if (nextSampleIndex >= BUFFER_SIZE) {
			nextSampleIndex = 0;
		}
		sampleCount++;
	}
}
