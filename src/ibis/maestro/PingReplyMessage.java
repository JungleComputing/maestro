package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message to tell the master the result of a ping message.
 * 
 * @author Kees van Reeuwijk.
 */
class PingReplyMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    PingReplyMessage( ReceivePortIdentifier worker ){
	super( worker );
    }

    /**
     * Returns a string representation of this ping reply message.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "Ping reply message from worker " + source;
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TransmissionEvent.Type getMessageType()
    {
	return TransmissionEvent.Type.PING_REPLY;
    }
}
