package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;

/**
 * A message in the Maestro system.
 * 
 * All message contain an id and a source port that together uniquely
 * identify them.
 * 
 * @author Kees van Reeuwijk
 *
 */
public abstract class Message implements Serializable {

    /** Contractual obligation. */
    private static final long serialVersionUID = 1547379144090317151L;
    protected final long messageId;
    
    /** The source of this message. */
    public final ReceivePortIdentifier source;
    private static long serialno = 0;

    private static long getID() {
	return serialno++;
    }

    /**
     * Constructs a new message from the given source.
     * We also generate a message id on the spot.
     * @param source The source of this message.
     */
    public Message( ReceivePortIdentifier source ) {
	this.messageId = getID();
	this.source = source;
    }

    /** Build a trace event for this message.
     * 
     * @param dest The destination of this message.
     * @return The constructed trace event.
     */
    public TraceEvent buildSendTraceEvent( ReceivePortIdentifier dest, long jobid )
    {
	return new TraceEvent( System.nanoTime(), source, dest, getMessageType(), true, messageId, jobid );
    }

    /** Build a trace event for this message.
     * 
     * @param dest The destination of this message.
     * @return The constructed trace event.
     */
    public TraceEvent buildReceiveTraceEvent( ReceivePortIdentifier dest, long jobid )
    {
        return new TraceEvent( System.nanoTime(), source, dest, getMessageType(), false, messageId, jobid );
    }

    protected abstract TraceEvent.Type getMessageType();
}