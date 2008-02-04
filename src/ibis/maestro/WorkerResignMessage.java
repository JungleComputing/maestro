package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message to tell the master not to send jobs to this worker any more.
 * 
 * @author Kees van Reeuwijk.
 */
class WorkerResignMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    // FIXME: inline this
    ReceivePortIdentifier getPort() { return source; }
    
    WorkerResignMessage( ReceivePortIdentifier worker ){
	super( worker );
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TransmissionEvent.Type getMessageType()
    {
	return TransmissionEvent.Type.RESIGN;
    }
}
