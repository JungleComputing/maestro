package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * The list of workers of a master.
 * 
 * @author Kees van Reeuwijk
 */
final class NodeList {
    private final ArrayList<NodeInfo> nodes = new ArrayList<NodeInfo>();
    private final TaskInfoList taskInfoList;

    NodeList( TaskInfoList taskInfoList )
    {
        this.taskInfoList = taskInfoList;
    }

    private NodeInfo getNode( NodeIdentifier workerIdentifier )
    {
        int ix = workerIdentifier.value;
        
        if( ix>=nodes.size() ) {
            return null;
        }
        return nodes.get( ix );
    }

    private static NodeInfo searchNode( List<NodeInfo> nodes, IbisIdentifier id )
    {
        for( NodeInfo w: nodes ) {
            if( w != null ) {
                if( w.hasIbisIdentifier( id ) ) {
                    return w;
                }
            }
        }
        return null;
    }

    /**
     * Add a node to our node administration.
     * We may, or may not, have info for this node. Create or update an entry. 
     * Add some registration info to the node info we already have.
     * @param port The receive port of this node.
     * @param types The types it supports.
     * @param theirIdentifierForUs Their identifier for us.
     * @return Our local identifier of this node.
     */
    synchronized NodeInfo subscribeNode( ReceivePortIdentifier port, TaskType[] types, NodeIdentifier theirIdentifierForUs )
    {
        final IbisIdentifier ibis = port.ibisIdentifier();
        NodeInfo node = searchNode( nodes, ibis );
        if( node == null ) {
            NodeIdentifier id = NodeIdentifier.getNextIdentifier();
            boolean local = Globals.localIbis.identifier().equals( ibis );

            // The node isn't in our administration, add it.
            node = new NodeInfo( id, ibis, local );
            if( Settings.traceRegistration ) {
                Globals.log.reportProgress( "Ibis " + ibis + " isn't in our admistration; creating new entry " + node );
            }
        }
        node.setTheirIdentifierForUs( theirIdentifierForUs );
        for( TaskType t: types ) {
            TaskInfo info = taskInfoList.getTaskInfo( t );
            NodeTaskInfo wti = node.registerTaskType( info );
            info.addWorker( wti );
        }
        if( Settings.traceNodeProgress || Settings.traceRegistration ){
            System.out.println( "Subscribing node " + node.ourIdentifierForNode + " theirIdentifierForUs=" + theirIdentifierForUs );
        }
        return node;
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
        NodeInfo wi = searchNode( nodes, theIbis );

        if( wi != null ) {
            orphans = wi.setDead();
        }
        return orphans;
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Remove any workers on that ibis.
     * @param theIbis The worker that is gone.
     */
    synchronized ArrayList<TaskInstance> removeNode( NodeIdentifier identifier )
    {
        if( Settings.traceWorkerList ) {
            System.out.println( "remove node " + identifier );
        }
        NodeInfo wi = getNode( identifier );
        ArrayList<TaskInstance> orphans = null;
        if( wi != null ) {
            orphans = wi.setDead();
        }
        return orphans;
    }

    /**
     * Register a task result in the info of the worker that handled it.
     * @param result The task result.
     */
    synchronized void registerTaskCompleted( TaskCompletedMessage result, long arrivalMoment )
    {
        NodeInfo w = getNode( result.source );
        if( w == null ) {
            Globals.log.reportError( "Worker status message from unknown worker " + result.source );
            return;
        }
        w.registerTaskCompleted( result, arrivalMoment );
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
        for( NodeInfo wi: nodes ) {
            wi.printStatistics( out );
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
    long getAverageCompletionTime( TaskType taskType )
    {
        TaskInfo taskInfo = taskInfoList.getTaskInfo( taskType );
        return taskInfo.getAverageCompletionTime();
    }

    synchronized void registerCompletionInfo( NodeIdentifier nodeID, WorkerQueueInfo[] workerQueueInfo, CompletionInfo[] completionInfo, long arrivalMoment )
    {
        NodeInfo w = getNode( nodeID );
        w.registerWorkerInfo( workerQueueInfo, completionInfo, arrivalMoment );
        w.registerAsCommunicating();
    }

    /** Given a remote node, returns the identifier this node uses for us.
     * @param nodeID The node to get the identifier for us for for.
     * @return The identifier for this node on the given node.
     */
    NodeIdentifier getTheirIdentifierForUs( NodeIdentifier nodeID )
    {
        NodeInfo w = getNode( nodeID );
        if( w == null ) {
            return null;
        }
        return w.getTheirIdentifierForUs();
    }

    int size()
    {
        return nodes.size();
    }

    void setPingStartMoment( NodeIdentifier workerID )
    {
        NodeInfo w = getNode( workerID );
        w.setPingStartMoment( System.nanoTime() );
    }

    synchronized protected void resetReservations()
    {
        for( NodeInfo wi: nodes ) {
            wi.resetReservations();
        }
    }

    protected synchronized void setSuspect( IbisIdentifier theIbis )
    {
        NodeInfo wi = searchNode( nodes, theIbis );

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
        int size = nodes.size();
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
            res = nodes.get( ix );
            if( res.isReady() ) {
                return res;
            }
            ix++;
            if( ix>=nodes.size() ) {
                // Wrap around.
                ix = 0;
            }
            n--;
        }
        // We tried all elements in the list with no luck.
        return null;
    }

    synchronized NodeInfo get( NodeIdentifier id )
    {
        return getNode( id );
    }

    /** Add a new node to the list with the given ibis identifier.
     * @param theIbis The identifier o the ibis.
     * @param local Is this a local node?
     * @return The newly created Node info for this node.
     */
    synchronized NodeInfo registerNode( IbisIdentifier theIbis, boolean local )
    {
        NodeInfo info = searchNode( nodes, theIbis );
        if( info != null ) {
            return info;
        }
        NodeIdentifier id = NodeIdentifier.getNextIdentifier();
        int ix = id.value;
        while( nodes.size()<=ix ) {
            nodes.add( null );
        }
        info = new NodeInfo( id, theIbis, local );
        nodes.set( ix, info );
        return info;
    }

    synchronized boolean registerAsCommunicating( NodeIdentifier id )
    {
        NodeInfo nodeInfo = getNode( id );
        if( nodeInfo == null ) {
            return false;
        }
        return nodeInfo.registerAsCommunicating();
    }

    /** FIXME.
     * @param theIbis
     * @return
     */
    synchronized boolean hasReadyNode( IbisIdentifier theIbis )
    {
        NodeInfo nodeInfo = searchNode( nodes, theIbis );
        if( nodeInfo == null ) {
            return false;
        }
        return nodeInfo.isReady();
    }
}