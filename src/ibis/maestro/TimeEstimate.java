package ibis.maestro;

import java.util.Random;

/**
 * This class maintains a time estimate based on collected samples.
 *
 * @author Kees van Reeuwijk.
 */
class TimeEstimate
{
    private long value;
    private long stdDev;
    private final Random rng = new Random();

    TimeEstimate()
    {
        value = 0l;
        stdDev = 0l;
    }

    /**
     * Returns a string representation of this estimate.
     */
    @Override
    public String toString()
    {
        return "average=" + Service.formatNanoseconds( value ) + " stdDev=" + Service.formatNanoseconds( stdDev );
    }

    /**
     * Returns a time estimate based on the current samples.
     * This method returns random number with uniform distribution between
     * the current minimum and maximum value in this list of samples.
     * @return The time estimate.
     */
    long getEstimate()
    {
        long res = (long) (value + stdDev*((2*rng.nextFloat())-1.0));
        return Math.max( 0L, res );
    }

    /**
     * Returns a reasonable average time estimate.
     * @return The average time.
     */
    long getAverage()
    {
        return value;
    }

    /**
     * Adds a new sample value to the estimate.
     * @param val The new sample value to add.
     */
    void addSample( long val )
    {
        value = (value+val)/2;
        long diff = Math.abs( value-val );
        stdDev = (stdDev+diff)/2;
    }
}
