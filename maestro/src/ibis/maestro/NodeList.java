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
    private UpDownCounter readyNodeCounter = new UpDownCounter( 0 );

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
        info = new NodeInfo( theIbis, taskInfoList, local );
        ibisToNodeMap.put( theIbis, info );
        return info;
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
        if( taskInfo == null ) {
            return null;
        }
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
        if( taskInfo == null ) {
            return Long.MAX_VALUE;
        }
        return taskInfo.getAverageCompletionTime();
    }

    int size()
    {
        return ibisToNodeMap.size();
    }

    protected synchronized void setSuspect( IbisIdentifier theIbis )
    {
        NodeInfo wi = ibisToNodeMap.get( theIbis );

        if( wi != null ) {
            wi.setSuspect();
        }
    }

    /** FIXME.
     * @param source
     * @return
     */
    private NodeInfo getNodeInfo( IbisIdentifier source )
    {
        NodeInfo nodeInfo = ibisToNodeMap.get( source );
        if( nodeInfo == null ) {
            nodeInfo = registerNode( source, false );  // This must be a remote node, since we certainly have registered the local node.
        }
        return nodeInfo;
    }

    synchronized NodeInfo get( IbisIdentifier id )
    {
        return getNodeInfo( id );
    }

    synchronized boolean registerAsCommunicating( IbisIdentifier ibisIdentifier )
    {
        NodeInfo nodeInfo = getNodeInfo( ibisIdentifier );
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

    void registerNodeUpdateInformation( NodeUpdateInfo l )
    {
        NodeInfo nodeInfo = getNodeInfo( l.source );
        nodeInfo.registerNodeInfo( l.workerQueueInfo, l.completionInfo );
    }

    void registerNodeUpdateInformation( NodeUpdateInfo[] l )
    {
        for( NodeUpdateInfo info: l ) {
            registerNodeUpdateInformation( info );
        }
    }
}
