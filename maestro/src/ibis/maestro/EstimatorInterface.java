package ibis.maestro;

import java.io.PrintStream;

interface EstimatorInterface {

	/**
	 * If we don't have a better estimate, use this one.
	 * 
	 * @param v
	 *            The new initial time estimate.
	 */
	void setInitialEstimate(double v);

	/**
	 * Returns the current average of the estimate.
	 * 
	 * @return The current average.
	 */
	double getAverage();

	/**
	 * Based on the current data, return a likely value for a next sample. The
	 * returned value may be random, so don't assume that for subsequent calls
	 * the same value is returned, even if no new samples have been added.
	 * 
	 * @return A likely value.
	 */
	double getLikelyValue();

	void printStatistics(PrintStream s, String lbl);

	/**
	 * Adds a new sample to the estimate.
	 * 
	 * @param v
	 *            The sample to add.
	 */
	void addSample(double v);

	double getStdDev();

	String getName();

	double getPessimisticEstimate();

	int getSampleCount();

}