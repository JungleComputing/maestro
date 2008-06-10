package ibis.maestro;

/**
 * This class maintains a time estimate based on collected samples.
 *
 * @author Kees van Reeuwijk.
 */
class TimeEstimate
{
    private long average;
    private long stdDev;

    TimeEstimate( long initial )
    {
        average = initial;
        stdDev = 0l;
    }

    /**
     * Returns a string representation of this estimate.
     */
    @Override
    public String toString()
    {
        return "average=" + Service.formatNanoseconds( average ) + " stdDev=" + Service.formatNanoseconds( stdDev );
    }

    /**
     * Returns a time estimate based on the current samples.
     * This method returns random number with uniform distribution between
     * the current minimum and maximum average in this list of samples.
     * @return The time estimate.
     */
    long getEstimate()
    {
	// FIXME: remove this method.
        return average;
    }

    /**
     * Returns a reasonable average time estimate.
     * @return The average time.
     */
    long getAverage()
    {
        return average;
    }

    /**
     * Adds a new sample average to the estimate.
     * @param val The new sample average to add.
     */
    void addSample( long val )
    {
        average = (2*average+val)/3;
        long diff = Math.abs( average-val );
        stdDev = (3*stdDev+diff)/4;
    }
}
