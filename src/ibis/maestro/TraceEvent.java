package ibis.maestro;


/**
 * A trace event.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class TraceEvent {

    protected final long time;

    /**
     * @param time
     */
    public TraceEvent( long time )
    {
	super();
	this.time = time;
    }

}