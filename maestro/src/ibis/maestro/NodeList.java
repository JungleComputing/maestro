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
    private final ArrayList<NodeInfo> workers = new ArrayList<NodeInfo>();
    private final ArrayList<TaskInfoOnMaster> taskInfoList = new ArrayList<TaskInfoOnMaster>();

    /**
     * Returns the TaskInfoOnMaster instance for the given task type. If
     * necessary, extend the taskInfoList to cover this type. If necessary,
     * create a new class instance for this type.
     * @param type The type we want info for.
     * @return The info structure for the given type.
     */
    TaskInfoOnMaster getTaskInfo( TaskType type )
    {
        int ix = type.index;
        while( ix+1>taskInfoList.size() ) {
            taskInfoList.add( null );
        }
        TaskInfoOnMaster res = taskInfoList.get( ix );
        if( res == null ) {
            res = new TaskInfoOnMaster( type );
            taskInfoList.set( ix, res );
        }
        return res;
    }
    
    private static NodeInfo searchWorker( List<NodeInfo> workers, NodeIdentifier workerIdentifier )
    {
        for( int i=0; i<workers.size(); i++ ) {
            NodeInfo w = workers.get( i );

            if( w.localIdentifier.equals( workerIdentifier ) ) {
                return w;
            }
        }
        return null;
    }

    private static NodeInfo searchWorker( List<NodeInfo> workers, IbisIdentifier id )
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
    NodeIdentifier subscribeNode( ReceivePortIdentifier port, TaskType[] types,
        NodeIdentifier masterIdentifier )
    {
        final IbisIdentifier ibis = port.ibisIdentifier();
        NodeInfo node = searchWorker( workers, ibis );
        if( node == null ) {
            
        }
        node.setPort( port );
        node.setIdentifierOnNode( masterIdentifier );
        for( TaskType t: types ) {
            TaskInfoOnMaster info = getTaskInfo( t );
            WorkerTaskInfo wti = node.getTaskInfo( t );
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
    ArrayList<TaskInstance> removeWorker( IbisIdentifier theIbis )
    {
        if( Settings.traceWorkerList ) {
            System.out.println( "remove worker " + theIbis );
        }
	ArrayList<TaskInstance> orphans = null;
        NodeInfo wi = searchWorker( workers, theIbis );

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
    ArrayList<TaskInstance> removeWorker( NodeIdentifier identifier )
    {
        if( Settings.traceWorkerList ) {
            System.out.println( "remove worker " + identifier );
        }
        NodeInfo wi = searchWorker( workers, identifier );
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
    void registerTaskCompleted( TaskCompletedMessage result, long arrivalMoment )
    {
        NodeInfo w = searchWorker( workers, result.source );
        if( w == null ) {
            Globals.log.reportError( "Worker status message from unknown worker " + result.source );
            return;
        }
        w.registerTaskCompleted( result, arrivalMoment );
    }

    /**
     * Returns true iff all workers are in a state that allow this master to finish.
     * @return True iff the master can finish.
     */
    boolean allowMasterToFinish()
    {
        for( NodeInfo w: workers ) {
            if( !w.allowMasterToFinish() ) {
                return false;
            }
        }
        return true;
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
    WorkerTaskInfo selectBestWorker( TaskType type )
    {
        TaskInfoOnMaster taskInfo = getTaskInfo( type );
        WorkerTaskInfo worker = taskInfo.getBestWorker();
        return worker;
    }

    /**
     * Given a print stream, print some statistics about the workers
     * to this stream.
     * @param out The stream to print to.
     */
    void printStatistics( PrintStream out )
    {
        for( NodeInfo wi: workers ) {
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
        TaskInfoOnMaster taskInfo = getTaskInfo( taskType );
        return taskInfo.getAverageCompletionTime();
    }

    void registerCompletionInfo( NodeIdentifier workerID, WorkerQueueInfo[] workerQueueInfo, CompletionInfo[] completionInfo, long arrivalMoment )
    {
        NodeInfo w = workers.get( workerID.value );
        w.registerWorkerInfo( workerQueueInfo, completionInfo, arrivalMoment );	
    }

    /** Given a worker, return the identifier of this master on the worker.
     * @param workerID The worker to get the identifier for.
     * @return The identifier of this master on the worker.
     */
    NodeIdentifier getMasterIdentifier( NodeIdentifier workerID )
    {
        // TODO: rename this method to getIdentifierOnNode
        NodeInfo w = workers.get( workerID.value );
        return w.getIdentifierOnNode();
    }

    int size()
    {
        return workers.size();
    }

    void setPingStartMoment( NodeIdentifier workerID )
    {
        NodeInfo w = workers.get( workerID.value );
        w.setPingStartMoment( System.nanoTime() );
    }

    protected void resetReservations()
    {
	for( NodeInfo wi: workers ) {
	    wi.resetReservations();
	}
    }

    protected void setSuspect( IbisIdentifier theIbis )
    {
	NodeInfo wi = searchWorker( workers, theIbis );

	if( wi != null ) {
	    wi.setSuspect();
	}
    }

    protected void setUnsuspect( IbisIdentifier theIbis )
    {
        NodeInfo wi = searchWorker( workers, theIbis );

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
        NodeInfo w = workers.get( workerID.value );
        if( w != null ) {
            w.setUnsuspect();
        }
    }

}
