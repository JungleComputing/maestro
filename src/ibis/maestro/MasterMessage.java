package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;

/**
 * Abstract superclass of messages that the master sends to the worker.
 * @author Kees van Reeuwijk
 *
 */
public abstract class MasterMessage extends Message implements Serializable {

    MasterMessage( ReceivePortIdentifier source ){
	super( source );
    }
}
