package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A entry in a message queue, containing a message and a destination.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class QueuedMessage {
    final IbisIdentifier destination;
    final Message msg;

    /**
     * @param destination
     * @param msg
     */
    QueuedMessage(final IbisIdentifier destination, final Message msg) {
	super();
	this.destination = destination;
	this.msg = msg;
    }

}
