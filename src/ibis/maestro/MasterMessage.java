package ibis.maestro;


/**
 * Abstract superclass of messages that the master sends to the worker.
 * @author Kees van Reeuwijk
 *
 */
public abstract class MasterMessage extends Message {

    MasterMessage( int source ){
	super( source );
    }
}
