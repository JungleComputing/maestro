package ibis.maestro;

import java.io.Serializable;

class TimeEstimate implements Serializable {
    private static final long serialVersionUID = 1L;
    static final TimeEstimate ZERO = new TimeEstimate(0, 0);
    final double mean;
    final double variance;

    TimeEstimate(double mean, double variance) {
        this.mean = mean;
        this.variance = variance;
    }

    TimeEstimate addIndependent(TimeEstimate b) {
        // TODO: can we just add standard deviations?
        return new TimeEstimate(mean + b.mean, variance + b.variance);
    }

    TimeEstimate multiply(double c) {
        return new TimeEstimate(c * mean, c * c * variance);
    }

    double getPessimisticEstimate() {
        return mean + Math.sqrt(variance);
    }

}
