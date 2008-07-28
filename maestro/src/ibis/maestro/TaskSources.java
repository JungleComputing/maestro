package ibis.maestro;

import java.util.ArrayList;

/**
 * A list of potential sources of extra work.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskSources
{
    /** The list of nodes we should ask for extra work if we are bored. */
    private final ArrayList<NodeInfo> taskSources = new ArrayList<NodeInfo>();

    synchronized void add( NodeInfo info ) {
        if( info != null && !taskSources.contains( info ) ) {
            taskSources.add( info );
        }
    }

    /** Get a random work source from the set.
     * @return
     */
    synchronized NodeInfo getRandomWorkSource()
    {
        NodeInfo res = null;

        while( true ){
            int size = taskSources.size();
            if( size == 0 ){
                return null;
            }
            // There are masters on the explict task sources list,
            // draw a random one.
            int ix = Globals.rng.nextInt( size );
            res = taskSources.remove( ix );
            if( !res.isSuspect() ){
                return res;
            }
            // The master we drew from the lottery is suspect or dead. Try again.
            // Note that the suspect task source has been removed from the list.
        }
    }

    synchronized boolean isEmpty()
    {
        return taskSources.isEmpty();
    }
}
