package ibis.maestro;

/**
 * This class maintains a time estimate based on collected samples.
 *
 * @author Kees van Reeuwijk.
 */
public class TimeEstimate
{
    private static final int SAMPLE_COUNT = 20;
    private static final long samples[] = new long[SAMPLE_COUNT];
    private int updateIndex = 0;
    private int minIndex;
    private int maxIndex;

    TimeEstimate( long min, long max )
    {
    }
    
    /**
     * Adds a new sample value to the estimate.
     * @param val
     */
    void addSample( long val )
    {
        samples[updateIndex++] = val;
    }
}
