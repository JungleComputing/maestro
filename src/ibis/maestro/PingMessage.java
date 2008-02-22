package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message sent to a worker to test its response time.
 * 
 * @author Kees van Reeuwijk
 */
public class PingMessage extends MasterMessage {
    /** */
    private static final long serialVersionUID = -933843016931228878L;

    /**
     * Constructs a new ping message. 
     * @param master The master that sends this ping message.
     */
    public PingMessage( final ReceivePortIdentifier master )
    {
	super( master );
    }


    /**
     * Returns a string representation of this ping message.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "Ping message. Reply to " + source;
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TransmissionEvent.Type getMessageType()
    {
	return TransmissionEvent.Type.PING;
    }
}
