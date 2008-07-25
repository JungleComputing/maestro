package ibis.maestro;



/**
 * A message to tell the master not to send tasks to this worker any more.
 * 
 * @author Kees van Reeuwijk.
 */
final class NodeResignMessage extends Message {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    final NodeIdentifier source;

    NodeResignMessage( NodeIdentifier worker ){
	source = worker;
    }
}
