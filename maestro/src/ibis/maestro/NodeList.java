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
    private final ArrayList<TaskInfo> taskInfoList = new ArrayList<TaskInfo>();

    /**
     * Returns the TaskInfoOnMaster instance for the given task type. If
     * necessary, extend the taskInfoList to cover this type. If necessary,
     * create a new class instance for this type.
     * @param type The type we want info for.
     * @return The info structure for the given type.
     */
    TaskInfo getTaskInfo( TaskType type )
    {
        int ix = type.index;
        while( ix+1>taskInfoList.size() ) {
            taskInfoList.add( null );
        }
        TaskInfo res = taskInfoList.get( ix );
        if( res == null ) {
            res = new TaskInfo( type );
            taskInfoList.set( ix, res );
        }
        return res;
    }

    private NodeInfo getNode( NodeIdentifier workerIdentifier )
    {
        int ix = workerIdentifier.value;
        
        if( ix>=nodes.size() ) {
            return null;
        }
        return nodes.get( ix );
    }

    private static NodeInfo searchNode( List<NodeInfo> workers, IbisIdentifier id )
    {
        for( NodeInfo w: workers ) {
            if( w != null ) {
                if( w.hasIbisIdentifier( id ) ) {
                    return w;
                }
            }
        }
        return null;
    }

    /**
     * Add some registration info to the node info we already have.
     * @param port
     * @param types
     * @param masterIdentifier
     * @return Our local identifier of this node.
     */
    synchronized NodeIdentifier subscribeNode( ReceivePortIdentifier port, TaskType[] types,
        NodeIdentifier masterIdentifier )
    {
        final IbisIdentifier ibis = port.ibisIdentifier();
        NodeInfo node = searchNode( nodes, ibis );
        if( node == null ) {
            Globals.log.reportInternalError( "Somebody replied to a registration request we didn't send" );
            return null;
        }
        node.setPort( port );
        node.setIdentifierOnNode( masterIdentifier );
        for( TaskType t: types ) {
            TaskInfo info = getTaskInfo( t );
            NodeTaskInfo wti = node.getTaskInfo( t );
            info.addWorker( wti );
        }
        if( Settings.traceMasterProgress ){
            System.out.println( "Subscribing node " + masterIdentifier );
        }
        return node.localIdentifier;
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
        TaskInfo taskInfo = getTaskInfo( type );
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
        TaskInfo taskInfo = getTaskInfo( taskType );
        return taskInfo.getAverageCompletionTime();
    }

    synchronized void registerCompletionInfo( NodeIdentifier workerID, WorkerQueueInfo[] workerQueueInfo, CompletionInfo[] completionInfo, long arrivalMoment )
    {
        NodeInfo w = nodes.get( workerID.value );
        w.registerWorkerInfo( workerQueueInfo, completionInfo, arrivalMoment );	
    }

    /** Given a worker, return the identifier of this master on the worker.
     * @param workerID The worker to get the identifier for.
     * @return The identifier of this master on the worker.
     */
    NodeIdentifier getNodeIdentifier( NodeIdentifier workerID )
    {
        NodeInfo w = nodes.get( workerID.value );
        return w.getIdentifierOnNode();
    }

    int size()
    {
        return nodes.size();
    }

    void setPingStartMoment( NodeIdentifier workerID )
    {
        NodeInfo w = nodes.get( workerID.value );
        w.setPingStartMoment( System.nanoTime() );
    }

    protected void resetReservations()
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

    protected synchronized void setUnsuspect( IbisIdentifier theIbis )
    {
        NodeInfo wi = searchNode( nodes, theIbis );

        if( wi != null ) {
            wi.setUnsuspect();
        }
    }

    /** Remove any suspect label from the given worker.
     * @param workerID The id of the worker that is no longer suspect.
     * @param node The node to report any change of state to.
     */
    protected void setUnsuspect( NodeIdentifier workerID, Node node )
    {
        if( workerID == null ){
            return;
        }
        NodeInfo w = nodes.get( workerID.value );
        if( w != null ) {
            w.setUnsuspect();
        }
    }

    /** FIXME.
     * @param source
     * @param port
     * @param identifierOnMaster
     */
    synchronized NodeInfo registerAccept( NodeIdentifier source, ReceivePortIdentifier port, NodeIdentifier identifierOnMaster )
    {
        NodeInfo wi = getNode( source );
        if( wi.isSuspect() && !wi.isDead() ) {
            wi.setUnsuspect();
        }
        return wi;
    }

    /** Returns a random registered master.
     * @return
     */
    synchronized NodeInfo getRandomRegisteredMaster()
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
            if( !res.isSuspect() && res.isRegistered() ) {
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

    /** FIXME.
     * @param masterId
     * @return
     */
    synchronized NodeInfo get( NodeIdentifier id )
    {
        return getNode( id );
    }

    /** Add a new node to the list with the given ibis identifier.
     * @param theIbis The identifier o the ibis.
     * @param local Is this a local node?
     * @return The newly created Node info for this node.
     */
    synchronized NodeInfo addNode( IbisIdentifier theIbis, boolean local )
    {
        NodeIdentifier id = NodeIdentifier.getNextIdentifier();
        int ix = id.value;
        while( nodes.size()<=ix ) {
            nodes.add( null );
        }
        NodeInfo info = new NodeInfo( id, theIbis, local );
        nodes.set( ix, info );
        return info;
    }

}
