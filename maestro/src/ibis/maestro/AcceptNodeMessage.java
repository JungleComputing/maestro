package ibis.maestro;


/**
 * A message from a worker to a master, telling it about its current
 * job completion times.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class AcceptNodeMessage extends Message
{
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    final NodeIdentifier source;
    
    final long sendMoment;

    /**
     * Constructs a new ping message.
     * @param source The identifier of the ping source.
     */
    AcceptNodeMessage( NodeIdentifier source, long sendMoment )
    {
        this.source = source;
        this.sendMoment = sendMoment;
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
