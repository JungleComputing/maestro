package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * The list of workers of a master.
 * 
 * @author Kees van Reeuwijk
 */
final class NodeList {
    private HashMap<IbisIdentifier,NodeInfo> ibisToNodeMap = new HashMap<IbisIdentifier, NodeInfo>();
    private final TaskInfoList taskInfoList;
    private UpDownCounter readyNodeCounter = new UpDownCounter();

    NodeList( TaskInfoList taskInfoList )
    {
        this.taskInfoList = taskInfoList;
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Remove any workers on that ibis.
     * @param theIbis The ibis that was gone.
     */
    synchronized ArrayList<TaskInstance> removeNode( IbisIdentifier theIbis )
    {
        if( Settings.traceWorkerList ) {
            System.out.println( "remove node " + theIbis );
        }
        ArrayList<TaskInstance> orphans = null;
        NodeInfo wi = ibisToNodeMap.get( theIbis );

        if( wi != null ) {
            orphans = wi.setDead();
        }
        readyNodeCounter.down();
        return orphans;
    }

    /** Add a new node to the list with the given ibis identifier.
     * @param theIbis The identifier o the ibis.
     * @param local Is this a local node?
     * @return The newly created Node info for this node.
     */
    synchronized NodeInfo registerNode( IbisIdentifier theIbis, boolean local )
    {
	NodeInfo info = ibisToNodeMap.get( theIbis );
        if( info != null ) {
            return info;
        }
        info = new NodeInfo( theIbis, local );
        ibisToNodeMap.put( theIbis, info );
        return info;
    }

    /**
     * Add a node to our node administration.
     * We may, or may not, have info for this node. Create or update an entry. 
     * Add some registration info to the node info we already have.
     * @param ibis The receive port of this node.
     * @param types The types it supports.
     * @return Our local identifier of this node.
     */
    synchronized NodeInfo subscribeNode( IbisIdentifier ibis, TaskType[] types, PacketSendPort sendPort )
    {
        NodeInfo node = ibisToNodeMap.get( ibis );
        if( node == null ) {
            boolean local = Globals.localIbis.identifier().equals( ibis );
    
            // The node isn't in our administration, add it.
            node = new NodeInfo( ibis, local );
            if( Settings.traceRegistration ) {
                Globals.log.reportProgress( "Ibis " + ibis + " isn't in our admistration; creating new entry " + node );
            }
        }
        sendPort.registerDestination( ibis );
        for( TaskType t: types ) {
            TaskInfo info = taskInfoList.getTaskInfo( t );
            NodeTaskInfo wti = node.registerTaskType( info );
            info.addWorker( wti );
        }
        if( Settings.traceNodeProgress || Settings.traceRegistration ){
            System.out.println( "Subscribing node " + ibis );
        }
        readyNodeCounter.up();
        return node;
    }

    /**
     * Register a task result in the info of the worker that handled it.
     * @param result The task result.
     */
    synchronized void registerTaskCompleted( TaskCompletedMessage result )
    {
        NodeInfo w = ibisToNodeMap.get( result.source );
        if( w == null ) {
            Globals.log.reportError( "Worker status message from unknown worker " + result.source );
            return;
        }
        w.registerTaskCompleted( result );
        w.registerAsCommunicating();
    }

    /**
     * Given a task type, select the best worker from the list that has a
     * free slot. In this context 'best' is simply the worker with the
     * shortest round-trip interval.
     *  
     * @param type The type of task we want to execute.
     * @return The info of the best worker for this task, or <code>null</code>
     *         if there currently aren't any workers for this task type.
     */
    NodeTaskInfo selectBestWorker( TaskType type )
    {
        TaskInfo taskInfo = taskInfoList.getTaskInfo( type );
        NodeTaskInfo worker = taskInfo.getBestWorker();
        return worker;
    }

    /**
     * Given a print stream, print some statistics about the workers
     * to this stream.
     * @param out The stream to print to.
     */
    void printStatistics( PrintStream out )
    {
	for( Map.Entry<IbisIdentifier, NodeInfo> entry : ibisToNodeMap.entrySet() ) {
	    NodeInfo wi = entry.getValue();
            if( wi != null ) {
                wi.printStatistics( out );
            }
        }
    }

    /**
     * Given a task type, return the estimated average time it will take
     * to execute this task and all subsequent tasks in the job by
     * the fastest route.
     * 
     * @param taskType The type of the task.
     * @return The estimated time in nanoseconds.
     */
    synchronized long getAverageCompletionTime( TaskType taskType )
    {
        TaskInfo taskInfo = taskInfoList.getTaskInfo( taskType );
        return taskInfo.getAverageCompletionTime();
    }

    int size()
    {
        return ibisToNodeMap.size();
    }

    synchronized protected void resetReservations()
    {
	for( Map.Entry<IbisIdentifier, NodeInfo> entry : ibisToNodeMap.entrySet() ) {
	    NodeInfo nodeInfo = entry.getValue();

	    if( nodeInfo != null ) {
                nodeInfo.resetReservations();
            }
        }
    }

    protected synchronized void setSuspect( IbisIdentifier theIbis )
    {
        NodeInfo wi = ibisToNodeMap.get( theIbis );

        if( wi != null ) {
            wi.setSuspect();
        }
    }

    /** Returns a random registered master.
     * @return
     */
    synchronized NodeInfo getRandomReadyNode()
    {
        NodeInfo res = null;
        int size = ibisToNodeMap.size();
        if( size == 0 ) {
            // No masters at all, give up.
            return null;
        }
        int ix = Globals.rng.nextInt( size );
        int n = size;
        while( n>0 ) {
            // We have picked a random place in the list of known masters, don't
            // return a dead or unregistered one, so keep walking the list until we
            // encounter a good one.
            // We only try 'n' times, since the list may consist entirely of duds.
            // (And yes, these duds skew the probabilities, we don't care.)
            res = ibisToNodeMap.get( ix );
            if( res.isReady() ) {
                return res;
            }
            ix++;
            if( ix>=ibisToNodeMap.size() ) {
                // Wrap around.
                ix = 0;
            }
            n--;
        }
        // We tried all elements in the list with no luck.
        return null;
    }

    synchronized NodeInfo get( IbisIdentifier id )
    {
        return ibisToNodeMap.get( id );
    }

    synchronized boolean registerAsCommunicating( IbisIdentifier ibisIdentifier )
    {
        NodeInfo nodeInfo = ibisToNodeMap.get( ibisIdentifier );
        if( nodeInfo == null ) {
            return false;
        }
        return nodeInfo.registerAsCommunicating();
    }

    /**
     * Wait until at least the given number of nodes have been registered with this node.
     * Since nodes will never register themselves instantaneously with other nodes,
     * the first jobs that are submitted may be executed on the first available node, instead
     * of the best one. Waiting until there is some choice can therefore be an advantage.
     * Of course, it must be certain that the given number of nodes will ever join the computation.
     * @param n The number of nodes to wait for.
     */
    int waitForReadyNodes( int n, long waittime )
    {
        return readyNodeCounter.waitForGreaterOrEqual( n, waittime );
    }

    /**
     * Check the deadlines of the nodes.
     */
    synchronized void checkDeadlines( long now )
    {
	for( Map.Entry<IbisIdentifier, NodeInfo> entry : ibisToNodeMap.entrySet() ) {
	    NodeInfo nodeInfo = entry.getValue();

	    if( nodeInfo != null ) {
                nodeInfo.checkDeadlines( now );
            }
        }
    }
}
