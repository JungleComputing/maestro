package ibis.maestro;

import java.io.Serializable;

class TimeEstimate implements Serializable {
	private static final long serialVersionUID = 1L;
	static final TimeEstimate ZERO = new TimeEstimate(0, 0);
	final double mean;
	final double variance;

	TimeEstimate(final double mean, final double variance) {
		this.mean = mean;
		this.variance = variance;
	}

	TimeEstimate addIndependent(final TimeEstimate b) {
		// TODO: can we just add standard deviations?
		return new TimeEstimate(mean + b.mean, variance + b.variance);
	}

	TimeEstimate multiply(final double c) {
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
