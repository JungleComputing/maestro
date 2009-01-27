package ibis.maestro;

/**
 * This class maintains a time estimate based on collected samples.
 * 
 * @author Kees van Reeuwijk.
 */
class TimeEstimate {
    private long average;

    private boolean initial = true;

    /**
     * Constructs a new time estimate with the given initial value.
     * 
     * @param initial
     *            The initial value of the time estimate.
     */
    TimeEstimate(long initial) {
        setInitialEstimate(initial);
    }

    /**
     * Returns a string representation of this estimate.
     */
    @Override
    public String toString() {
        return Utils.formatNanoseconds(average);
    }

    /**
     * Returns a reasonable average time estimate.
     * 
     * @return The average time.
     */
    long getAverage() {
        return average;
    }

    /**
     * Adds a new sample average to the estimate.
     * 
     * @param val
     *            The new sample average to add.
     */
    void addSample(long val) {
        if (initial) {
            average = val;
            initial = false;
        } else {
            average = (3 * average + val) / 4;
        }
    }

    /**
     * If we don't have a better estimate, use this one.
     * 
     * @param v
     *            The new initial time estimate.
     */
    public void setInitialEstimate(long v) {
        if (initial) {
            average = v / 2; // This estimate is also inaccurate, so
                                // underestimate a bit.
        }
    }
}
