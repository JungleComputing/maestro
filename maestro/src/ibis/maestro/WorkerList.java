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

    static final double RESEARCH_BUDGET_FOR_NEW_WORKER = 2.0;
    static final double RESEARCH_BUDGET_PER_TASK = 0.05;

    private TaskInfoOnMaster getTaskInfo( TaskType type )
    {
        int ix = type.index;
        while( ix+1>taskInfoList.size() ) {
            taskInfoList.add( null );
        }
        TaskInfoOnMaster res = taskInfoList.get( ix );
        if( res == null ) {
            res = new TaskInfoOnMaster( workers.size()*RESEARCH_BUDGET_FOR_NEW_WORKER );
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

    private static int searchWorker( List<WorkerInfo> workers, ReceivePortIdentifier id ) {
        for( int i=0; i<workers.size(); i++ ) {
            WorkerInfo w = workers.get(i);
            if( w.port.equals( id ) ) {
                return i;
            }
        }
        return -1;
    }

    WorkerIdentifier subscribeWorker( ReceivePortIdentifier me, ReceivePortIdentifier workerPort, boolean local, MasterIdentifier identifierForWorker, TaskType[] types )
    {
        Master.WorkerIdentifier workerID = new Master.WorkerIdentifier( workers.size() );
        WorkerInfo worker = new WorkerInfo( workerPort, workerID, identifierForWorker, local, types );

        for( TaskInfoOnMaster info: taskInfoList ){
            if( info != null ){
                info.addResearchBudget( RESEARCH_BUDGET_FOR_NEW_WORKER );
            }
        }
        if( Settings.traceMasterProgress ){
            System.out.println( "Master " + me + ": subscribing worker " + workerID + "; identifierForWorker=" + identifierForWorker + "; local=" + local );
        }
        workers.add( worker );
        return workerID;
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Remove any workers on that ibis.
     * @param theIbis The ibis that was gone.
     */
    void removeWorker( IbisIdentifier theIbis )
    {
        WorkerInfo wi = searchWorker( workers, theIbis );
        if( wi != null ) {
            wi.setDead();
        }
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

    /** Returns true iff we have a worker on our list with the
     * given identifier.
     * @param identifier The identifier to search for.
     * @return True iff we know the given worker.
     */
    boolean contains( WorkerIdentifier identifier )
    {
        WorkerInfo i = searchWorker( workers, identifier );
        return i != null;
    }

    /**
     * Register a task result in the info of the worker that handled it.
     * @param result The task result.
     */
    void registerTaskCompleted( TaskCompletedMessage result )
    {
        WorkerInfo w = searchWorker( workers, result.source );
        if( w == null ) {
            System.err.println( "Worker status message from unknown worker " + result.source );
            return;
        }
        w.registerTaskCompleted( result );
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
     * @param taskType The type of task we want to execute.
     * @return The info of the best worker for this task, or <code>null</code>
     *         if there currently aren't any workers for this task type.
     */
    WorkerInfo selectBestWorker( TaskType taskType )
    {
        WorkerInfo best = null;
        long bestInterval = Long.MAX_VALUE;
        int competitors = 0;
        int idleWorkers = 0;

        for( int i=0; i<workers.size(); i++ ) {
            WorkerInfo wi = workers.get( i );

            if( !wi.isDead() ) {
                if( wi.isIdle( taskType ) ) {
                    idleWorkers++;
                }
                competitors++;
                long val = wi.estimateJobCompletion( taskType );

                if( val<Long.MAX_VALUE ) {
                    if( Settings.traceRemainingJobTime ) {
                        System.out.println( "Worker " + wi + ": task type " + taskType + ": estimated completion time " + Service.formatNanoseconds( val ) );
                    }
                    if( val<bestInterval ) {
                        bestInterval = val;
                        best = wi;
                    }
                }
            }
        }
        TaskInfoOnMaster taskInfo = getTaskInfo( taskType );
        taskInfo.addResearchBudget( RESEARCH_BUDGET_PER_TASK );
        if( Settings.traceRemainingJobTime || Settings.traceMasterProgress ) {
            System.out.println( "Master: competitors=" + competitors + "; taskInfo=" + taskInfo );
        }

        if( best == null || (idleWorkers>0 && taskInfo.canResearch()) ) {
            // We can't find a worker for this task. See if there is
            // a disabled worker we can enable.
            long bestTime = Long.MAX_VALUE;
            WorkerInfo candidate = null;

            for( int i=0; i<workers.size(); i++ ) {
                WorkerInfo wi = workers.get( i );

                if( wi.isIdle( taskType ) ) {
                    long t = wi.getOptimisticRoundtripTime( taskType );
                    if( t<bestTime ) {
                        candidate = wi;
                        bestTime = t;
                    }
                }
            }
            if( candidate != null && candidate!=best ) {
                taskInfo.useResearchBudget();
                if( Settings.traceMasterQueue ) {
                    Globals.log.reportProgress( "Trying worker " + candidate + "; taskInfo=" + taskInfo );
                }
                best = candidate;
            }
        }
        if( best == null ) {
            if( Settings.traceMasterQueue ){
                int busy = 0;
                int notSupported = 0;
                for( WorkerInfo wi: workers ){
                    if( wi.supportsType( taskType ) ){
                        busy++;
                    }
                    else {
                        notSupported++;
                    }
                }
                Globals.log.reportProgress( "No best worker (" + busy + " busy, " + notSupported + " not supporting) for task of type " + taskType );
            }
        }
        else {
            if( Settings.traceMasterQueue ){
        	Globals.log.reportProgress( "Selected " + best + " for task of type " + taskType + "; estimated job completion time " + Service.formatNanoseconds( bestInterval ) + "; taskInfo=" + taskInfo );
            }
        }
        return best;
    }

    /**
     * Given a worker, return true iff we know about this worker.
     * @param worker The worker we want to know about.
     * @return True iff this is a known worker.
     */
    boolean isKnownWorker( ReceivePortIdentifier worker )
    {
        int ix = searchWorker( workers, worker );
        return ix>=0;
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
        long res = Long.MAX_VALUE;

        for( WorkerInfo wi: workers ) {
            long val = wi.getAverageCompletionTime( taskType );

            if( val<res ) {
                res = val;
            }
        }
        return res;
    }

    void registerCompletionInfo( WorkerIdentifier workerID, WorkerQueueInfo[] workerQueueInfo, CompletionInfo[] completionInfo )
    {
        WorkerInfo w = workers.get( workerID.value );
        w.registerWorkerInfo( workerQueueInfo, completionInfo );	
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
