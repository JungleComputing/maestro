package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.Master.WorkerIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * The list of workers of a master.
 * 
 * @author Kees van Reeuwijk
 */
final class WorkerList {
    private final ArrayList<WorkerInfo> workers = new ArrayList<WorkerInfo>();
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
    
    private static WorkerInfo searchWorker( List<WorkerInfo> workers, WorkerIdentifier workerIdentifier )
    {
        for( int i=0; i<workers.size(); i++ ) {
            WorkerInfo w = workers.get( i );

            if( w.identifier.equals( workerIdentifier ) ) {
                return w;
            }
        }
        return null;
    }

    private static WorkerInfo searchWorker( List<WorkerInfo> workers, IbisIdentifier id )
    {
        for( int i=0; i<workers.size(); i++ ) {
            WorkerInfo w = workers.get(i);
            if( w.port.ibisIdentifier().equals( id ) ) {
                return w;
            }
        }
        return null;
    }

    WorkerIdentifier subscribeWorker( ReceivePortIdentifier me, ReceivePortIdentifier workerPort, boolean local, int workThreads, MasterIdentifier identifierForWorker, TaskType[] types )
    {
        Master.WorkerIdentifier workerID = new Master.WorkerIdentifier( workers.size() );
        WorkerInfo worker = new WorkerInfo( this, workerPort, workerID, identifierForWorker, local, workThreads, types );

        for( TaskType t: types ) {
            TaskInfoOnMaster info = getTaskInfo( t );
            WorkerTaskInfo wti = worker.getTaskInfo( t );
            info.addWorker( wti );
        }
        if( Settings.traceMasterProgress ){
            System.out.println( "Master " + me + ": subscribing worker " + workerID + " identifierForWorker=" + identifierForWorker + " local=" + local + " workThreads=" + workThreads );
        }
        workers.add( worker );
        return workerID;
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Remove any workers on that ibis.
     * @param theIbis The ibis that was gone.
     */
    ArrayList<TaskInstance> removeWorker( IbisIdentifier theIbis )
    {
	ArrayList<TaskInstance> orphans = null;
        WorkerInfo wi = searchWorker( workers, theIbis );

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
    void removeWorker( WorkerIdentifier identifier )
    {
        WorkerInfo wi = searchWorker( workers, identifier );
        if( wi != null ) {
            wi.setDead();
        }
    }

    /**
     * Register a task result in the info of the worker that handled it.
     * @param result The task result.
     */
    void registerTaskCompleted( TaskCompletedMessage result, long arrivalMoment )
    {
        WorkerInfo w = searchWorker( workers, result.source );
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
        for( WorkerInfo w: workers ) {
            if( !w.allowsMasterToFinish() ) {
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

    /** Given a worker identifier, declare it dead.
     * @param workerID The worker to declare dead.
     */
    void declareDead( WorkerIdentifier workerID )
    {
        WorkerInfo w = workers.get( workerID.value );
        w.setDead();
    }

    /**
     * Return the number of known workers.
     * @return The number of known workers.
     */
    int getWorkerCount()
    {
        return workers.size();
    }

    /**
     * Given a print stream, print some statistics about the workers
     * to this stream.
     * @param out The stream to print to.
     */
    void printStatistics( PrintStream out )
    {
        for( WorkerInfo wi: workers ) {
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

    void registerCompletionInfo( WorkerIdentifier workerID, WorkerQueueInfo[] workerQueueInfo, CompletionInfo[] completionInfo, long arrivalMoment )
    {
        WorkerInfo w = workers.get( workerID.value );
        w.registerWorkerInfo( workerQueueInfo, completionInfo, arrivalMoment );	
    }

    /** Given a worker, return the identifier of this master on the worker.
     * @param workerID The worker to get the identifier for.
     * @return The identifier of this master on the worker.
     */
    MasterIdentifier getMasterIdentifier( WorkerIdentifier workerID )
    {
        WorkerInfo w = workers.get( workerID.value );
        return w.identifierWithWorker;
    }

    int size()
    {
        return workers.size();
    }

    void setPingStartMoment( WorkerIdentifier workerID )
    {
        WorkerInfo w = workers.get( workerID.value );
        w.setPingStartMoment( System.nanoTime() );
    }

    protected void resetReservations()
    {
	for( WorkerInfo wi: workers ) {
	    wi.resetReservations();
	}
    }

}
