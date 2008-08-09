package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Gossip information.
 *
 * @author Kees van Reeuwijk.
 */
class Gossip
{
    private ArrayList<NodeUpdateInfo> gossipList = new ArrayList<NodeUpdateInfo>();

    synchronized GossipMessage constructMessage( IbisIdentifier target, boolean needsReply )
    {
        NodeUpdateInfo content[] = getCopy();
        return new GossipMessage( target, content, needsReply );
    }

    synchronized boolean isEmpty()
    {
        return gossipList.isEmpty();
    }

    synchronized NodeUpdateInfo[] getCopy()
    {
        NodeUpdateInfo content[] = new NodeUpdateInfo[gossipList.size()];
        for( int i=0; i<content.length; i++ ) {
            content[i] = gossipList.get( i ).getDeepCopy();
        }
        return content;
    }

    private int searchInfo( IbisIdentifier ibis )
    {
        for( int ix = 0; ix<gossipList.size(); ix++ ) {
            NodeUpdateInfo i = gossipList.get( ix );

            if( i.source.equals( ibis ) ) {
                return ix;
            }
        }
        return -1;
    }

    /**
     * Returns the best average completion time for this task.
     * We compute this by taking the minimum over all our workers.
     * @return The best average completion time of our workers.
     */
    synchronized long getBestCompletionTimeAfterMasterQueue( int ix, int nextIx )
    {
        long res = Long.MAX_VALUE;

        for( NodeUpdateInfo node: gossipList ) {
            // FIXME: take into account the transmission time to each worker.
            long val = node.getCompletionOnWorker( ix, nextIx );

            if( val<res ) {
                res = val;
            }
        }
        return res;
    }

    /**
     * Given the current queue intervals on the master, recompute
     * in-place the completion intervals for the various task types.
     * The completion interval is defined as the time it will take
     * a task on
     * a given master from the moment it enters its master queue to
     * the moment its entire job is completed.
     * @param masterQueueIntervals The time in nanoseconds for each task it is estimated to dwell on the master queue.
     */
    synchronized void recomputeCompletionTimes( long masterQueueIntervals[], JobList jobs )
    {
        int [][] indexLists = jobs.getIndexLists();
        int ix = searchInfo( Globals.localIbis.identifier() );
        if( ix<0 ) {
            // We're not in the table yet. Don't worry.
            return;
        }
        // Note that we recompute the times back to front, since we will need the later
        // times to compute the earlier times.
        
        NodeUpdateInfo localInfo = gossipList.get( ix );

        for( int indexList[] : indexLists ) {
            int nextIndex = -1;
            for( int typeIndex: indexList ) {
                long t = Service.safeAdd( masterQueueIntervals[typeIndex], getBestCompletionTimeAfterMasterQueue( typeIndex, nextIndex ) );
                localInfo.completionInfo[typeIndex] = t;
                nextIndex = typeIndex;
            }
        }
        localInfo.timeStamp = System.nanoTime();
    }

    /** Registers the given information in our collection of gossip.
     * @param update The information to register.
     * @return True iff we learned something new.
     */
    synchronized boolean register( NodeUpdateInfo update )
    {
        int ix = searchInfo( update.source );
        if( ix>=0 ) {
            // This is an update for the same node.
            NodeUpdateInfo i = gossipList.get( ix );

            if( update.timeStamp>i.timeStamp ) {
                // This is more recent info, overwrite the old entry.
                if( Settings.traceGossip ) {
                    Globals.log.reportProgress( "Updated gossip info about " + update.source );
                }
                gossipList.set( ix, update );
                return true;
            }
            return false;
        }
        if( Settings.traceGossip ) {
            Globals.log.reportProgress( "Got info about new node " + update.source );
        }
        // If we reach this point, we didn't have info about this node.
        gossipList.add( update );
        this.notifyAll();  // Wake any waiters for ready nodes
        return true;
    }

    synchronized void removeInfoForNode( IbisIdentifier ibis )
    {
        int ix = searchInfo( ibis );

        if( ix>=0 ) {
            gossipList.remove( ix );
        }        
    }

    /** Overwrite the worker queue info of our local information with the new info.
     * @param update
     * @param idleProcessors 
     */
    synchronized void registerWorkerQueueInfo( WorkerQueueInfo[] update, int idleProcessors, int numberOfProcessors )
    {
        IbisIdentifier ourIbis = Globals.localIbis.identifier();
        int ix = searchInfo( ourIbis );
        if( ix<0 ) {
            long completionInfo[] = new long[Globals.numberOfTaskTypes];
            Arrays.fill( completionInfo, Long.MAX_VALUE );
            NodeUpdateInfo localInfo = new NodeUpdateInfo( completionInfo, update, ourIbis, idleProcessors, numberOfProcessors );
            gossipList.add( localInfo );
            return;
        }
        // Note that we recompute the times back to front, since we will need the later
        // times to compute the earlier times.
        
        NodeUpdateInfo localInfo = gossipList.get( ix );
        
        localInfo.workerQueueInfo = update;
        localInfo.timeStamp = System.nanoTime();
        localInfo.idleProcessors = idleProcessors;
    }

    synchronized NodeUpdateInfo getLocalUpgate()
    {
        IbisIdentifier ourIbis = Globals.localIbis.identifier();
        int ix = searchInfo( ourIbis );
        if( ix<0 ) {
            Globals.log.reportInternalError( "No local info in gossip???" );
            return null;
        }
        return gossipList.get( ix ).getDeepCopy();
    }
    
    synchronized void print( PrintStream s )
    {
        for( NodeUpdateInfo entry: gossipList ) {
            entry.print( s );
        }
    }

    synchronized int size()
    {
        return gossipList.size();
    }

    /**
     * Given a number of nodes to wait for, keep waiting until we have gossip information about
     * at least this many nodes, or until the given time has elapsed.
     * @param nodes The number of nodes to wait for.
     * @param maximalWaitTime The maximal time in ms to wait for these nodes.
     * @return The actual number of nodes there was information for at the moment we stopped waiting.
     */
    int waitForReadyNodes( int nodes, long maximalWaitTime )
    {
        final long deadline = System.currentTimeMillis()+maximalWaitTime;
        while( true ) {
            long now = System.currentTimeMillis();
            long sleepTime = Math.max( 1L, deadline-now );
            synchronized( this ) {
                int sz = size();
                if( sz>=nodes || now>deadline ) {
                    return sz;
                }
                try{
                    wait( sleepTime );
                }
                catch( InterruptedException e ){
                    // Ignore
               }
            }
        }
    }
}
