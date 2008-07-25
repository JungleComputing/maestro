package ibis.maestro;

import ibis.ipl.Ibis;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.HashSet;

/**
 * The master administration in the Maestro flow graph framework.
 * 
 * @author Kees van Reeuwijk
 * 
 */
@SuppressWarnings("synthetic-access")
class Master
{
    final Node node;
    final MasterQueue queue;

    private long nextTaskId = 0;
    private long incomingTaskCount = 0;
    private long handledTaskCount = 0;
    private final long startTime;
    private long stopTime = 0;

    /**
     * A worker identifier.
     * This is in essence just an int, but we encapsulate it to make
     * sure we don't mix it up with other kinds of identifier that
     * we use.
     * @author Kees van Reeuwijk
     *
     */
    static final class WorkerIdentifier implements Serializable {
        private static final long serialVersionUID = 3271311796768467853L;
        final int value;

        WorkerIdentifier( int value )
        {
            this.value = value;
        }

        /**
         * Returns the hash code of this worker identifier.
         * @return A hash code for this identifier.
         */
        @Override
        public int hashCode() {
            return value;
        }

        /**
         * Returns true iff this worker identifier is equal to the given
         * one.
         * @param obj The object to compare to.
         * @return True iff the two identifiers are equal.
         */
        @Override
        public boolean equals( Object obj )
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final WorkerIdentifier other = (WorkerIdentifier) obj;
            return (value == other.value);
        }

        /** Returns a string representation of this worker.
         * 
         * @return The string representation.
         */
        @Override
        public String toString()
        {
            return "W" + value;
        }
    }

    /** Creates a new master instance.
     * @param ibis The Ibis instance this master belongs to.
     * @param node The node this master belongs to.
     */
    Master( Ibis ibis, Node node )
    {
        this.queue = new MasterQueue();
        this.node = node;
        startTime = System.nanoTime();
    }

    /** On a locked queue, try to send out as many task as we can. */
    private void drainQueue()
    {
        Submission sub = new Submission();
        long taskId;
        int reserved = 0;  // How many tasks are reserved for future submission.
        HashSet<TaskType> noReadyWorkers = new HashSet<TaskType>();

        if( Settings.traceMasterProgress ){
            System.out.println( "Master: submitting all possible tasks" );
        }
        node.nodes.resetReservations();   // FIXME: store reservations in a separate structure.
        while( true ) {
            if( queue.isEmpty() ) {
                // Mission accomplished.
                return;
            }
            reserved = queue.selectSubmisson( reserved, sub, node.nodes, noReadyWorkers );
            WorkerTaskInfo wti = sub.worker;
            TaskInstance task = sub.task;
            if( wti == null ){
                break;
            }
            WorkerInfo worker = wti.worker;
            taskId = nextTaskId++;
            worker.registerTaskStart( task, taskId, sub.predictedDuration );
            if( Settings.traceMasterQueue ) {
                System.out.println( "Selected " + worker + " as best for task " + task );
            }

            RunTaskMessage msg = new RunTaskMessage( worker.identifierWithWorker, worker.identifier, task, taskId );
            long sz = node.sendPort.tryToSend( worker.identifier.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
            if( sz<0 ){
                // Try to put the paste back in the tube.
                // The sendport has already reported the trouble with the worker.
                worker.retractTask( msg.taskId );
                queue.add( msg.task );
            }
        }
    }

    /**
     * Adds the given task to the work queue of this master.
     * @param task The task instance to add to the queue.
     */
    void submit( TaskInstance task )
    {
        if( Settings.traceMasterProgress || Settings.traceMasterQueue ) {
            System.out.println( "Master: received task " + task );
        }
        synchronized ( queue ) {
            incomingTaskCount++;
            queue.add( task );
            drainQueue();
            queue.notifyAll();
        }
    }

    /** Print some statistics about the entire master run. */
    void printStatistics( PrintStream s )
    {
        if( stopTime<startTime ) {
            System.err.println( "Worker didn't stop yet" );
        }
        queue.printStatistics( s );
        long workInterval = stopTime-startTime;
        s.printf(  "Master: # incoming tasks = %5d\n", incomingTaskCount );
        s.printf(  "Master: # handled tasks  = %5d\n", handledTaskCount );
        s.println( "Master: run time         = " + Service.formatNanoseconds( workInterval ) );
    }

    CompletionInfo[] getCompletionInfo( JobList jobs, WorkerList workers )
    {
        synchronized( queue ) {
            return queue.getCompletionInfo( jobs, workers );
        }
    }

    void countHandledTask()
    {
        handledTaskCount++;
    }

    /**
     * Do all updates of the master adminstration that we can.
     * 
     */
    void updateAdministration()
    {
        synchronized( queue ){
            drainQueue();
        }
    }
}
