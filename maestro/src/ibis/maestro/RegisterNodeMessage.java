package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A message from a worker to a master, telling it that the worker exists, and which identifier the
 * worker wants the master to use when it talks to it.
 *
 * @author Kees van Reeuwijk
 *
 */
final class RegisterNodeMessage extends NonEssentialMessage
{
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** The identifier of the node that wants to register. */
    final IbisIdentifier ibis;

    final TaskType[] supportedTypes;

    final long padding[] = new long[Settings.PING_PADDING_SIZE];

    /** Our identifier for the node we're sending to. */
    final NodeIdentifier senderIdentifierForReceiver;

    /**
     * Constructs a new worker registration message.
     * @param ibis The ibis of the node.
     * @param supportedTypes The list of supported types of this node worker.
     * @param ourIdentifierForNode The identifier to use.
     */
    RegisterNodeMessage( IbisIdentifier ibis, TaskType[] supportedTypes, NodeIdentifier ourIdentifierForNode )
    {
	this.ibis = ibis;
	this.senderIdentifierForReceiver = ourIdentifierForNode;
	this.supportedTypes = supportedTypes;
	this.sendMoment = System.nanoTime();
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
        return "register worker " + ibis + "; supported types: [" + tl + ']';
    }
}
