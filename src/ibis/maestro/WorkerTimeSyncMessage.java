package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message sent to another node to synchronize the times .
 * 
 * @author Kees van Reeuwijk
 */
public class WorkerTimeSyncMessage extends WorkerMessage {
    /** */
    private static final long serialVersionUID = -933843016931228879L;

    /**
     * Constructs a new  time sync message. 
     * @param source The worker that sends this time sync message.
     */
    public WorkerTimeSyncMessage( final ReceivePortIdentifier source )
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
