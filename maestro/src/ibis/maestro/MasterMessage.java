package ibis.maestro;


/**
 * Abstract superclass of messages that the master sends to the worker.
 * @author Kees van Reeuwijk
 *
 */
public abstract class MasterMessage extends Message {
    final Worker.MasterIdentifier source;

    MasterMessage( Worker.MasterIdentifier source )
    {
	this.source = source;
    }
}
