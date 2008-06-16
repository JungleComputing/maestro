package ibis.maestro;

/**
 * This class maintains a time estimate based on collected samples.
 *
 * @author Kees van Reeuwijk.
 */
class TimeEstimate
{
    private long average;

    TimeEstimate( long initial )
    {
        average = initial;
    }

    /**
     * Returns a string representation of this estimate.
     */
    @Override
    public String toString()
    {
        return "average=" + Service.formatNanoseconds( average );
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
    }
}
