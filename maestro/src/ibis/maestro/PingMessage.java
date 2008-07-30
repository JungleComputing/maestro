package ibis.maestro;


/**
 * A message from a worker to a master, telling it about its current
 * job completion times.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class PingMessage extends Message
{
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    final NodeIdentifier source;
    
    final long sendMoment;
    
    final long padding[] = new long[Settings.PING_PADDING_SIZE];

    /**
     * Constructs a new ping message.
     * @param source The identifier of the ping source.
     */
    PingMessage(NodeIdentifier source, long sendTime )
    {
        this.source = source;
        this.sendMoment = sendTime;
    }

    /**
     * Returns a string representation of update message. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "Ping";
    }
}
