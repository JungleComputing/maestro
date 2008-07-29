package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message from a worker to a master, telling it that the worker exists, and which identifier the
 * worker wants the master to use when it talks to it.
 *
 * @author Kees van Reeuwijk
 *
 */
final class RegisterNodeMessage extends Message
{
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** Our receive port. */
    final ReceivePortIdentifier port;

    final TaskType[] supportedTypes;
    
    /** Our identifier for the node we're sending to. */
    final NodeIdentifier ourIdentifierForNode;

    /** Is this a reply to another registration? */
    final boolean reply;

    /**
     * Constructs a new worker registration message.
     * @param port The receive port to use to submit tasks.
     * @param supportedTypes The list of supported types of this node worker.
     * @param ourIdentifierForNode The identifier to use.
     */
    RegisterNodeMessage( ReceivePortIdentifier port, TaskType[] supportedTypes, NodeIdentifier ourIdentifierForNode, boolean reply )
    {
	this.port = port;
	this.ourIdentifierForNode = ourIdentifierForNode;
	this.supportedTypes = supportedTypes;
	this.reply = reply;
    }

    /**
     * Returns a string representation of this message.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        String tl = "";
        
        for( TaskType t: supportedTypes ) {
            if( !tl.isEmpty() ) {
                tl += ',';
            }
            tl += t;
        }
        return "register worker " + port + "; supported types: [" + tl + ']';
    }
}
