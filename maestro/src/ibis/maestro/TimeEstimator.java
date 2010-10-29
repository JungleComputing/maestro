package ibis.maestro;

import java.io.PrintStream;

/**
 * This class maintains a time estimate based on collected samples.
 * 
 * @author Kees van Reeuwijk.
 */
class DecayingEstimator implements EstimatorInterface {
	private double average;

	private boolean initial = true;

	/**
	 * Constructs a new time estimate with the given initial value.
	 * 
	 * @param initial
	 *            The initial value of the time estimate.
	 */
	DecayingEstimator(final double initial) {
		setInitialEstimate(initial);
	}

	/**
	 * Returns a string representation of this estimate.
	 */
	@Override
	public String toString() {
		return Utils.formatSeconds(average);
	}

	/**
	 * Returns a reasonable average time estimate.
	 * 
	 * @return The average time.
	 */
	@Override
	public double getAverage() {
		return average;
	}

	/**
	 * Returns a reasonable estimate.
	 * 
	 * @return The average time.
	 */
	@Override
	public double getLikelyValue() {
		return average;
	}

	/**
	 * Adds a new sample average to the estimate.
	 * 
	 * @param val
	 *            The new sample average to add.
	 */
	@Override
	public void addSample(final double val) {
		if (initial) {
			average = val;
			initial = false;
		} else {
			average = (3 * average + val) / 4;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ibis.maestro.EstimateInterface#setInitialEstimate(double)
	 */
	@Override
	public void setInitialEstimate(final double v) {
		if (initial) {
			// This estimate is also inaccurate, so
			// underestimate a bit.
			average = v / 2;
		}
	}

	@Override
	public void printStatistics(final PrintStream s, final String lbl) {
		if (lbl != null) {
			s.print(lbl);
			s.print(": ");
		}
		s.println("average=" + average + " initial=" + initial);
	}

}
