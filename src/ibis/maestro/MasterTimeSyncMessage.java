package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message sent to another node to synchronize the times .
 * 
 * @author Kees van Reeuwijk
 */
public class MasterTimeSyncMessage extends MasterMessage {
    /** */
    private static final long serialVersionUID = -933843016931228879L;

    /**
     * Constructs a new ping message. 
     * @param source The master that sends this ping message.
     */
    public MasterTimeSyncMessage( final ReceivePortIdentifier source )
    {
	super( source );
    }

    /**
     * Returns a string representation of this ping message.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "Time sync message from master " + source;
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TransmissionEvent.Type getMessageType()
    {
	return TransmissionEvent.Type.TIME_SYNC;
    }
}
