package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * Abstract superclass of messages that the master sends to the worker.
 * @author Kees van Reeuwijk
 *
 */
public abstract class WorkerMessage extends Message {

    /** Contractual obligation. */
    private static final long serialVersionUID = 1547379144090317151L;
    
    WorkerMessage( ReceivePortIdentifier source ){
	super( source );
    }
}
