package ibis.maestro;

import java.io.Serializable;

public class TimeEstimate implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final TimeEstimate ZERO = new TimeEstimate(0, 0);
	final double mean;
	final double variance;

	public TimeEstimate(final double mean, final double variance) {
		this.mean = mean;
		this.variance = variance;
	}

	public TimeEstimate addIndependent(final TimeEstimate b) {
		if (b == null) {
			return null;
		}
		return new TimeEstimate(mean + b.mean, variance + b.variance);
	}

	public TimeEstimate multiply(final double c) {
		return new TimeEstimate(c * mean, c * c * variance);
	}

	double getPessimisticEstimate() {
		return mean + Math.sqrt(variance);
	}

	double getLikelyValue() {
		// TODO: for low sample count the variation should be larger.
		final double stdDev = Math.sqrt(variance);
		return mean + stdDev * Globals.rng.nextGaussian();
	}

}
