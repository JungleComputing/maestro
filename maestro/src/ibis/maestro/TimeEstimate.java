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
    private long reference;

    /**
     * Constructs a new time estimate with the given initial value.
     * @param initial The initial value of the time estimate.
     */
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
        return Utils.formatNanoseconds( average );
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
     * @return <code>true</code> if there was a significant change in the value.
     */
    boolean addSample( long val )
    {
        if( initial ) {
            average = val;
            reference = val;
            initial = false;
            return true;
        }
        average = (2*average+val)/3;
        long diff = Math.abs( average-reference );
        if( diff>Math.abs( average )/10 ) {
            // More than 10% change compared to current reference.
            // This is a significant change.
            reference = average;
            return true;
        }
        return false;
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
