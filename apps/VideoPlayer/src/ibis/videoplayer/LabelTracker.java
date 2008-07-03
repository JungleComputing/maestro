package ibis.videoplayer;

import java.io.Serializable;
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
public class LabelTracker {
    private static long labelValue = 0L;
    private static final boolean trace = false;

    /** The first label not in the bulk range. */
    private long endOfRange = 0L;

    private static final TreeSet<Long> set = new TreeSet<Long>();

    /**
     * A label as handed out by the tracker. It is entirely opaque.
     */
    static class Label implements Serializable {
        private static final long serialVersionUID = 1L;
        private final long value;

	Label( long value )
	{
	    this.value = value;
	}
	
	/**
	 * Returns a string representation of this label.
	 * @return
	 */
	@Override
	public String toString()
	{
	    return "label#" + value;
	}
    }
    
    Label nextLabel()
    {
	Label res = new Label( labelValue++ );
        if( trace ){
            System.out.println( "nextLabel(): handed out label " + res );
        }
        return res;
    }
    
    /**
     * Returns the given label to our administration.
     * We're not shocked if the same label is returned more than once,
     * or out of order.
     * @param l The label we return.
     */
    @SuppressWarnings("synthetic-access")
    void returnLabel( Label l )
    {
        if( trace ){
            System.out.println( "returnLabel(): got back label " + l );
        }
	long val = l.value;
	if( val<endOfRange ) {
	    // Already covered by the range. Nothing to do.
	    return;
	}
	set.add( val );
	
	// Try to enlarge the range with elements in the set.
	while( set.contains( endOfRange ) ) {
	    set.remove( endOfRange );
	    endOfRange++;
	}
        if( trace ){
            System.out.println( "returnLabel(): endOfRange=" + endOfRange + "; set size: " + set.size() );
        }
    }

    public boolean allAreReturned()
    {
	return endOfRange == labelValue;
    }
}
