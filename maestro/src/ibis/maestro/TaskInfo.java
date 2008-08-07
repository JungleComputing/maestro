package ibis.maestro;

import java.util.LinkedList;
import java.util.List;

/**
 * Information on a task on the master.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskInfo
{
    private final List<NodeTaskInfo> workers = new LinkedList<NodeTaskInfo>();
    final TaskType type;

    TaskInfo( final TaskType type )
    {
        this.type = type;
    }

    /**
     * Returns a string representation of this task info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return String.format( "type=" + type + " workers: ", workers.size() );
    }


    /**
     * Returns the best average completion time for this task.
     * We compute this by taking the minimum over all our workers.
     * @return The best average completion time of our workers.
     */
    synchronized long getAverageCompletionTime()
    {
        long res = Long.MAX_VALUE;

        for( NodeTaskInfo wi: workers ) {
            long val = wi.getAverageCompletionTime();

            if( val<res ) {
                res = val;
            }
        }
        return res;
    }

    void registerNode( NodeInfo nodeInfo )
    {
        NodeTaskInfo nodeTaskInfo = nodeInfo.get( type );
        synchronized( this ) {
            if( nodeTaskInfo != null ) {
                workers.add( nodeTaskInfo );
            }
        }
    }

}
