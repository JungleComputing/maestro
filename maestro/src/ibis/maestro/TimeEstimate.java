package ibis.maestro;

/**
 * This class maintains a time estimate based on collected samples.
 *
 * @author Kees van Reeuwijk.
 */
class TimeEstimate
{
    private long average;
    private boolean initial = true;

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
        if( initial ) {
            average = val;
            initial = false;
        }
        else {
            average = (2*average+val)/3;
        }
    }

    /** If we don't have a better estimate, use this one.
     * @param v The new time estimate.
     */
    public void setInitialEstimate( long v )
    {
        if( initial ) {
            average = v;
        }
    }
}
