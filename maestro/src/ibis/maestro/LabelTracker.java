package ibis.maestro;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

/**
 * Hands out labels, and keeps track of the ones that have been returned.
 * Allows labels to be returned out of order and more than once. At all
 * times we can tell whether there are still outstanding labels, or
 * that all labels we have handed out have been returned.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class LabelTracker
{
    /* 
     * In principle we could just use a set, but as an optimization
     * we represent the range of labels starting from 0 as just
     * the upper value of this range, and we try to grow this range
     * whenever possible.
     * 
     * The remaining labels are stored in a set.
     */
    private long labelValue = 0L;
    private static final boolean TRACE = false;

    /** The first label not in the bulk range. */
    private long endOfRange = 0L;

    private final Set<Long> set = new TreeSet<Long>();

    /**
     * A label as handed out by the tracker. It should be treated as an opaque token.
     */
    public static class Label implements Serializable {
	private static final long serialVersionUID = 1L;
	private final long value;

	Label( final long value )
	{
	    this.value = value;
	}

	/**
	 * Returns a string representation of this label.
	 * @return The string representation of this label.
	 */
	@Override
	public String toString()
	{
	    return "label#" + value;
	}
    }

    /**
     * Issues the next label from the tracker.
     * @return The label.
     */
    public synchronized Label nextLabel()
    {
	final Label res = new Label( labelValue++ );
	if( TRACE ){
	    Globals.log.reportProgress( "nextLabel(): handed out " + res );
	}
	return res;
    }

    /**
     * Returns the given label to the tracker.
     * The labels do not have to be returned in the same order they were
     * issued, and the same label may be returned more than once.
     * @param lbl The label we return.
     * @return <code>true</code> iff the label had been returned before.
     */
    @SuppressWarnings("synthetic-access")
    public synchronized boolean returnLabel( final Label lbl )
    {
	if( TRACE ){
	    Globals.log.reportProgress( "returnLabel(): got back " + lbl );
	}
	final long val = lbl.value;
	if( val<endOfRange ) {
	    // Already covered by the range. Nothing to do.
	    notifyAll();
	    return true;
	}
	boolean duplicate = false;
	if( val == endOfRange ) {
	    // Don't put it in the set and take it out again
	    // for a (hopefully) common case.
	    endOfRange++;
	}
	else {
	    duplicate = set.contains(  val );
	    if( !duplicate ) {
	        set.add( val );
	    }
	}

	// Try to enlarge the range with elements in the set.
	while( set.contains( endOfRange ) ) {
	    set.remove( endOfRange );
	    endOfRange++;
	}
	if( TRACE ){
	    Globals.log.reportProgress( "returnLabel(): endOfRange=" + endOfRange + " labelValue=" + labelValue + " setsize: " + set.size() );
	}
	notifyAll();  // Wake any return waiters.
	return duplicate;
    }

    /**
     *  Returns true iff all labels we handed out have been returned.
     * @return True iff all handed out labels have been returned.
     */
    synchronized public boolean allAreReturned()
    {
	return endOfRange == labelValue;
    }

    /**
     * Wait until all labels have been returned.
     * The only way of getting this thread back is by returning all labels
     * that were handed out.
     * @throws InterruptedException Thrown if an interrupt was sent.
     */
    public void waitForAllLabels() throws InterruptedException
    {
	while( true ){
	    synchronized( this ){
		if( endOfRange == labelValue ){
		    break;
		}
		if( TRACE ){
		    Globals.log.reportProgress( "Waiting for labels: endOfRange=" + endOfRange + " labelValue=" + labelValue );
		}
		wait();
	    }
	}
	if( TRACE ){
	    Globals.log.reportProgress( "Got all labels back" );
	}
    }
    
    /**
     * Returns the total number of labels that have been issued.
     * @return the total number of labels that have been issued.
     */
    public synchronized long getIssuedLabels()
    {
        return labelValue;
    }
    
    /**
     * Returns the total number of labels that have been returned.
     * @return The total number of labels that have been returned.
     */
    public synchronized long getReturnedLabels()
    {
        return endOfRange + set.size();
    }
    
    /**
     * Returns an array containing all the still outstanding labels.
     *  @return An array containing all outstanding labels.
     */
    public synchronized Label[] listOutstandingLabels()
    {
        int sz = ((int)(labelValue-endOfRange))-set.size();
        Label res[] = new Label[sz];
        int ix = 0;
        
        for( long val=endOfRange; val<labelValue; val++ ) {
            if( !set.contains( val ) ) {
                res[ix++] = new Label( val );
            }
        }
        return res;
    }
}
