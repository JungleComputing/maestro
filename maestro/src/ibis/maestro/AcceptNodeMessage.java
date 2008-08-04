package ibis.maestro;

import ibis.ipl.IbisIdentifier;


/**
 * A message from a worker to a master, telling it about its current
 * job completion times.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class AcceptNodeMessage extends NonEssentialMessage
{
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    final IbisIdentifier source;
    
    final long pingSendMoment;

    /**
     * Constructs a new accept message.
     * @param dest The node to send the message to.
     * @param pingSendMoment The moment the original registration message (and ping message) was sent.
     */
    AcceptNodeMessage( IbisIdentifier dest, long pingSendMoment )
    {
	super( dest );
        this.source = Globals.localIbis.identifier();
        this.pingSendMoment = pingSendMoment;
    }

    /**
     * Returns a string representation of update message. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "Accept node message from " + source;
    }
}
