package ibis.maestro;

import java.util.Random;

/**
 * This class maintains a time estimate based on collected samples.
 *
 * @author Kees van Reeuwijk.
 */
public class TimeEstimate
{
    private static final int SAMPLE_WINDOW = 20;
    private final long sampleValues[] = new long[SAMPLE_WINDOW];
    private int updateIndex = 0;
    private int sampleCount = 0;
    private int minIndex;
    private int maxIndex;
    private final Random rng = new Random();

    TimeEstimate()
    {

    }

    /**
     * Returns a string representation of this estimate.
     */
    @Override
    public String toString()
    {
        if( sampleCount == 0 ){
            return "no information";
        }
        if( sampleCount == 1 ){
            return "one sample: " + Service.formatNanoseconds( sampleValues[minIndex] );
        }
        return Service.formatNanoseconds( sampleValues[minIndex] ) + "..." + Service.formatNanoseconds( sampleValues[maxIndex] );
    }

    /**
     * Returns a time estimate based on the current samples.
     * This method returns random number with uniform distribution between
     * the current minimum and maximum value in this list of samples.
     * @return The time estimate.
     */
    long getEstimate()
    {
        long min;
        long max;
        if( sampleCount == 0 ) {
            min = 0;
            max = 1000; // Completely arbitary: 1 us
        }
        else if( sampleCount == 1 ){
            // We only have one sample; add a bit of spread.
            min = 0;
            max = sampleValues[minIndex]*3;
        }
        else {
	    min = sampleValues[minIndex];
	    max = sampleValues[maxIndex];
        }
        return (long) (min + (max-min)*rng.nextFloat());
    }

    /**
     * Returns a time estimate based on the current samples.
     * This method returns random number with uniform distribution between
     * the current minimum and maximum value in this list of samples.
     * @return The time estimate.
     */
    long getEstimate( int n )
    {
        long min;
        long max;
        if( sampleCount == 0 ) {
            min = 0;
            max = n*1000; // Completely arbitary: 1 us
        }
        else if( sampleCount == 1 ){
            // We only have one sample; add a bit of spread.
            min = 0;
            max = n*sampleValues[minIndex]*3;
        }
        else {
            min = n*sampleValues[minIndex];
            max = n*sampleValues[maxIndex];
        }
        //return (long) (min + (max-min)*rng.nextFloat());
        return max;
    }

    /**
     * Adds a new sample value to the estimate.
     * @param val The new sample value to add.
     */
    void addSample( long val )
    {
        sampleValues[updateIndex] = val;
        if( sampleCount == 0 ) {
            // This is the first sample.
            minIndex = updateIndex;
            maxIndex = updateIndex;
            // Fill the entire array with this sample to avoid
            // special-casing the max and min calculations below.
            for( int i=0; i<SAMPLE_WINDOW; i++ ) {
                sampleValues[i] = val;
            }
        }
        if( minIndex == updateIndex ) {
            // We've just overwritten the current minimum, scan all
            // samples for the new one.
            minIndex = 0;
            for( int i=1; i<SAMPLE_WINDOW; i++ ) {
                if( sampleValues[i]<sampleValues[minIndex] ) {
                    minIndex = i;
                }
            }
        }
        else {
            if( sampleValues[updateIndex]<sampleValues[minIndex] ) {
                minIndex = updateIndex;
            }
        }
        if( maxIndex == updateIndex ) {
            // We've just overwritten the current maximum, scan all
            // samples for the new one.
            maxIndex = 0;
            for( int i=1; i<SAMPLE_WINDOW; i++ ) {
                if( sampleValues[i]>sampleValues[maxIndex] ) {
                    maxIndex = i;
                }
            }
        }
        else {
            if( sampleValues[updateIndex]>sampleValues[maxIndex] ) {
                maxIndex = updateIndex;
            }
        }
        sampleCount++;
        updateIndex++;
        if( updateIndex>=SAMPLE_WINDOW ) {
            updateIndex = 0;
        }
	if( false ){
	    for( int i=0; i<SAMPLE_WINDOW; i++ ){
		System.out.print( "sampleValues[" + i + "]=" + sampleValues[i] );
		if( i == minIndex ){
		    System.out.print( " <minIndex>" );
		}
		if( i == maxIndex ){
		    System.out.print( " <maxIndex>" );
		}
		if( i == updateIndex ){
		    System.out.print( " <updateIndex>" );
		}
		System.out.println();
	    }
	    System.out.println();
	}
    }
}
