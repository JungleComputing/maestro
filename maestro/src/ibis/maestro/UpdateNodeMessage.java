package ibis.maestro;



/**
 * A message from a worker to a master, telling it about its current
 * job completion times.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class UpdateNodeMessage extends Message
{
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    final NodePerformanceInfo update;

    /**
     * Constructs a new work request message.
     * @param identifier The identifier to use.
     * @param masterHasWork 
     */
    UpdateNodeMessage( NodePerformanceInfo update )
    {
        this.update = update;
    }

}
