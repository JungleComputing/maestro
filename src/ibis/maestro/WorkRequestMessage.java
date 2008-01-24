package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message telling a master that we would like to receive jobs.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class WorkRequestMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    // FIXME: inline this.
    ReceivePortIdentifier getPort() { return source; }
    
    WorkRequestMessage( ReceivePortIdentifier worker ){
	super( worker );
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TraceEvent.Type getMessageType()
    {
	return TraceEvent.Type.ASK_WORK;
    }
}
