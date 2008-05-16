package ibis.maestro;


/**
 * Abstract superclass of messages that the master sends to the worker.
 * @author Kees van Reeuwijk
 *
 */
public abstract class WorkerMessage extends Message {

    /** Contractual obligation. */
    private static final long serialVersionUID = 1547379144090317151L;
    final Master.WorkerIdentifier source;

    WorkerMessage( Master.WorkerIdentifier source ){
	this.source = source;
    }
}
