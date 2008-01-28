package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * Abstract superclass of messages that the master sends to the worker.
 * @author Kees van Reeuwijk
 *
 */
public abstract class MasterMessage extends Message {

    MasterMessage( ReceivePortIdentifier source ){
	super( source );
    }
}
