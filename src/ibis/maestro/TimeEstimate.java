package ibis.maestro;

import java.util.Random;

/**
 * This class maintains a time estimate based on collected samples.
 *
 * @author Kees van Reeuwijk.
 */
class TimeEstimate
{
    private static final int SAMPLE_WINDOW = 10;
    private final long sampleValues[] = new long[SAMPLE_WINDOW];
    private int updateIndex = 0;
    private int sampleCount = 0;
    private int minIndex;
    private int maxIndex;
    private final Random rng = new Random();

    TimeEstimate()
    {
        // The sample values array is filled with 0, which suits us fine.
	// Until we have filled the entire sample window, we make
	// a very optimistic assumption about the minimum time.
        maxIndex = SAMPLE_WINDOW-1;
        minIndex = SAMPLE_WINDOW-1;
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
            return "one sample: " + Service.formatNanoseconds( sampleValues[maxIndex] );
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
        long min = sampleValues[minIndex];
        long max = sampleValues[maxIndex];
        return (long) (min + (max-min)*rng.nextFloat());
    }

    /**
     * Returns a reasonable average time estimate.
     * @return The average time.
     */
    long getAverage()
    {
	// FIXME: use a true average.
	return (sampleValues[minIndex]+sampleValues[maxIndex])/2;
    }

    /**
     * Adds a new sample value to the estimate.
     * @param val The new sample value to add.
     */
    void addSample( long val )
    {
        sampleValues[updateIndex] = val;
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
