package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

/**
 * A message sent to a worker to tell it about new neighbor Ibises.
 * 
 * @author Kees van Reeuwijk
 */
public class AddNeighborsMessage extends MasterMessage {
    /** Contractual obligation */
    private static final long serialVersionUID = 1L;
    private final IbisIdentifier l[];

    /**
     * Constructs a new neighbor update message. 
     * @param source The source of this message.
     * @param l The list of new neighbor Ibises.
     */
    public AddNeighborsMessage( ReceivePortIdentifier source, IbisIdentifier l[])
    {
	super( source );
	this.l = l;
    }

    /**
     * Returns the list of neighbors in this message.
     * @return The list of neighbors.
     */
    public IbisIdentifier[] getNeighbors()
    {
        return l;
    }

    /**
     * @return The string representation of this message.
     */
    @Override
    public String toString()
    {
        String res = "AddNeighborsMessage[" ;
        boolean first = true;

        for( IbisIdentifier i: l ){
            if( first ){
                first = false;
            }
            else {
                res += "," + i;
            }
        }
        return res + ']';
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TraceEvent.Type getMessageType()
    {
	return TraceEvent.Type.ADD_NEIGHBORS;
    }
}
