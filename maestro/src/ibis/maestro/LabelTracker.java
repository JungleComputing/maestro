package ibis.maestro;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

/**
 * Hands out labels, and keeps track of the ones that have been returned.
 * Allow labels to be returned out of order and more than once. At all
 * times we can tell whether there are still outstanding labels, or
 * that all labels we have handed out have been returned.
 * 
 * The implementation assumes that the set of returned values is mainly,
 * but not entirely, in order. The bulk of the set is therefore represented
 * by a range starting from 0. Any out-of-order labels are stored
 * separately in a hashmap, that we try to transfer to the range
 * at all new arrivals of labels.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class LabelTracker
{
    private long labelValue = 0L;
    private static final boolean TRACE = false;

    /** The first label not in the bulk range. */
    private long endOfRange = 0L;

    private final Set<Long> set = new TreeSet<Long>();

    /**
     * A label as handed out by the tracker. It is entirely opaque.
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
     * Get the next label from the tracker.
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
     * Returns the given label to our administration.
     * We're not shocked if the same label is returned more than once,
     * or out of order.
     * @param lbl The label we return.
     */
    @SuppressWarnings("synthetic-access")
    public synchronized void returnLabel( final Label lbl )
    {
	if( TRACE ){
	    Globals.log.reportProgress( "returnLabel(): got back " + lbl );
	}
	final long val = lbl.value;
	if( val<endOfRange ) {
	    // Already covered by the range. Nothing to do.
	    notifyAll();
	    return;
	}
	if( val == endOfRange ) {
	    // Don't put it in the set and take it out again
	    // for a (hopefully) common case.
	    endOfRange++;
	}
	else {
	    set.add( val );
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
}
