package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * A master in the Maestro flow graph framework.
 * 
 * @author Kees van Reeuwijk
 * 
 */
@SuppressWarnings("synthetic-access")
public class Master extends Thread implements PacketReceiveListener<WorkerMessage>
{
    private final Node node;
    private final WorkerList workers = new WorkerList();
    private final PacketUpcallReceivePort<WorkerMessage> receivePort;
    private final LinkedList<WorkerIdentifier> acceptList = new LinkedList<WorkerIdentifier>();
    private final PacketSendPort<MasterMessage> sendPort;
    private final MasterQueue queue;

    private boolean stopped = false;
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
     * @throws IOException Thrown if the master cannot be created.
     */
    Master( Ibis ibis, Node node ) throws IOException
    {
        super( "Master" );
        this.queue = new MasterQueue();
        this.node = node;
        sendPort = new PacketSendPort<MasterMessage>( ibis, node );
        receivePort = new PacketUpcallReceivePort<WorkerMessage>( ibis, Globals.masterReceivePortName, this );
        startTime = System.nanoTime();
    }

    /**
     * Start this master thread.
     */
    @Override
    public void start()
    {
        receivePort.enable();           // We're open for business.
        super.start();                  // Start the thread
    }

    /**
     * Set the local listener to the given class instance.
     * @param localListener The local listener to use.
     */
    void setLocalListener( PacketReceiveListener<MasterMessage> localListener )
    {
        sendPort.setLocalListener( localListener );
    }

    void setStopped()
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Master is set to stopped state" );
        }
        synchronized( queue ) {
            stopped = true;
            queue.notifyAll();
        }
    }

    /**
     * Returns true iff this master is in stopped mode, has no
     * tasks in its queue, and has not outstanding tasks on its workers.
     * @return True iff this master has processed all tasks it ever will.
     */
    private boolean isFinished()
    {
        synchronized( queue ){
            if( !stopped ){
                return false;
            }
            if( !queue.isEmpty() ) {
                System.out.println( "Master set to stopped, but queue not empty." );
                return false;
            }
            if( !workers.allowMasterToFinish() ){
                System.out.println( "Master set to stopped, but workers are still busy." );
                return false;
            }
        }
        return true;
    }

    private void unsubscribeWorker( WorkerIdentifier worker )
    {
        if( Settings.traceWorkerList ) {
            System.out.println( "unsubscribe of worker " + worker );
        }
        synchronized( queue ){
            ArrayList<TaskInstance> orphans = workers.removeWorker( worker );
            queue.add( orphans );
            queue.notifyAll();
        }
    }

    /**
     * A worker has sent use a status message for a task. Process it.
     * @param result The status message.
     */
    private void handleTaskCompletedMessage( TaskCompletedMessage result, long arrivalMoment )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received a worker task completed message " + result );
        }
        synchronized( queue ){
            workers.registerTaskCompleted( result, arrivalMoment );
            handledTaskCount++;
        }
        // Force the queue to drain.
        submitAllPossibleTasks();
        synchronized( queue ) {
            // Now notify our thread main loop. It will do a
            // submitAllPossibleTasks() too, but that may take
            // some time. More importantly, it checks if we can
            // stop.
            queue.notifyAll();
        }
    }

    private void handleJobResultMessage( JobResultMessage m )
    {
        node.reportCompletion( m.job, m.result );
    }

    private boolean sendAcceptMessage( WorkerIdentifier workerID )
    {
        ReceivePortIdentifier myport = receivePort.identifier();
        Worker.MasterIdentifier idOnWorker = workers.getMasterIdentifier( workerID );
        WorkerAcceptMessage msg = new WorkerAcceptMessage( idOnWorker, myport, workerID );
        boolean ok = true;

        workers.setPingStartMoment( workerID );
        long sz = sendPort.tryToSend( workerID.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        if( sz<0 ){
            synchronized( queue ) {
                queue.notifyAll();
            }
            ok = false;
        }
        return ok;
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * 
     * @param m The update message.
     * @param arrivalMoment The time in ns the message arrived.
     */
    private void handleWorkerUpdateMessage( WorkerUpdateMessage m, long arrivalMoment )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received worker update message " + m );
        }
        synchronized( queue ){
            workers.registerCompletionInfo( m.source, m.workerQueueInfo, m.completionInfo, arrivalMoment );
            queue.notifyAll();
        }
    }

    /**
     * A worker has sent us a message to register itself with us. This is
     * just to tell us that it's out there. We tell it what our receive port is,
     * and which handle we have assigned to it, so that it can then inform us
     * of the types of tasks it supports.
     *
     * @param m The worker registration message.
     */
    private void handleRegisterWorkerMessage( RegisterWorkerMessage m )
    {
        WorkerIdentifier workerID;
        ReceivePortIdentifier worker = m.port;

        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Master: received registration message " + m + " from worker " + worker );
        }
        if( m.supportedTypes.length == 0 ) {
            Globals.log.reportInternalError( "Worker " + worker + " has zero supported types??" );
        }
        synchronized( queue ) {
            boolean local = sendPort.isLocalListener( m.port );
            workerID = workers.subscribeWorker( receivePort.identifier(), worker, local, m.workThreads, m.masterIdentifier, m.supportedTypes );
            sendPort.registerDestination( worker, workerID.value );
            acceptList.add( workerID );
            queue.notifyAll();
        }
    }

    /**
     * Returns true iff this listener is associated with the given port.
     * @param port The port it should be associated with.
     * @return True iff this listener is associated with the port.
     */
    @Override
    public boolean hasReceivePort( ReceivePortIdentifier port )
    {
        return port.equals( receivePort.identifier() );
    }

    /**
     * Handles message <code>msg</code> from worker.
     * @param msg The message we received.
     */
    @Override
    public void messageReceived( WorkerMessage msg, long arrivalMoment )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Master: received message " + msg );
        }
        workers.setUnsuspect( msg.source, node );
        if( msg instanceof TaskCompletedMessage ) {
            TaskCompletedMessage result = (TaskCompletedMessage) msg;

            handleTaskCompletedMessage( result, arrivalMoment );
        }
        else if( msg instanceof JobResultMessage ) {
            JobResultMessage m = (JobResultMessage) msg;

            handleJobResultMessage( m );
        }
        else if( msg instanceof WorkerUpdateMessage ) {
            WorkerUpdateMessage m = (WorkerUpdateMessage) msg;

            handleWorkerUpdateMessage( m, arrivalMoment );
        }
        else if( msg instanceof RegisterWorkerMessage ) {
            RegisterWorkerMessage m = (RegisterWorkerMessage) msg;

            handleRegisterWorkerMessage( m );
        }
        else if( msg instanceof WorkerResignMessage ) {
            WorkerResignMessage m = (WorkerResignMessage) msg;

            unsubscribeWorker( m.source );
        }
        else {
            Globals.log.reportInternalError( "the master should handle message of type " + msg.getClass() );
        }
    }

    /**
     * Submit all work currently in the queues until all workers are busy
     * or all work has been submitted.
     * @return True if we stop because all workers are busy, otherwise we stop because there is no work to dispatch.
     */
    private boolean submitAllPossibleTasks()
    {
        Subtask sub = new Subtask();
        long taskId;
        int reserved = 0;
        WorkerIdentifier workerToAccept = null;
        boolean stopBecauseBusy;
        HashSet<TaskType> noReadyWorkers = new HashSet<TaskType>();

        if( Settings.traceMasterProgress ){
            System.out.println( "Master: submitting all possible tasks" );
        }

        synchronized( queue ){
            workers.resetReservations();
            while( true ) {
                if( queue.isEmpty() ) {
                    stopBecauseBusy = false;
                    break;
                }
                reserved = queue.selectSubmisson( reserved, sub, workers, noReadyWorkers );
                WorkerTaskInfo wti = sub.worker;
                TaskInstance task = sub.task;
                if( wti == null ){
                    stopBecauseBusy = true;
                    break;
                }
                WorkerInfo worker = wti.worker;
                taskId = nextTaskId++;
                long allowanceDeadline = sub.predictedDuration*Settings.ALLOWANCE_DEADLINE_MARGIN;
                long rescheduleDeadline = sub.predictedDuration*Settings.RESCHEDULE_DEADLINE_MARGIN;
                worker.registerTaskStart( task, taskId, sub.predictedDuration, allowanceDeadline, rescheduleDeadline );
                if( Settings.traceMasterQueue ) {
                    System.out.println( "Selected " + worker + " as best for task " + task );
                }

                RunTaskMessage msg = new RunTaskMessage( worker.identifierWithWorker, worker.identifier, task, taskId );
                long sz = sendPort.tryToSend( worker.identifier.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
                if( sz<0 ){
                    // Try to put the paste back in the tube.
                    worker.retractTask( msg.taskId );
                    queue.add( msg.task );
                }
            }
            if( !acceptList.isEmpty() ) {
        	workerToAccept = acceptList.remove();
            }
        }
        if( workerToAccept != null ) {
            boolean ok = sendAcceptMessage( workerToAccept );
            if( !ok ) {
        	// Couldn't send an accept message, back on the queue.
        	synchronized( queue ) {
        	    acceptList.add(workerToAccept);
        	}
            }
        }
        if( Settings.traceWorkerSelection ){
            System.out.println( "-- end of submitAllPossibleTasks() -- stopBecauseBusy=" + stopBecauseBusy );
        }
        return stopBecauseBusy;
    }

    /**
     * Adds the given task to the work queue of this master.
     * @param task The task instance to add to the queue.
     */
    void submit( TaskInstance task )
    {
        if( Settings.traceMasterProgress || Settings.traceMasterQueue) {
            System.out.println( "Master: received task " + task );
        }
        synchronized ( queue ) {
            incomingTaskCount++;
            queue.add( task );
        }
        submitAllPossibleTasks();
    }

    /** Runs this master. */
    @Override
    public void run()
    {
        if( Settings.traceMasterProgress ){
            System.out.println( "Starting master thread" );
        }
        while( true ){
            boolean everyoneBusy = submitAllPossibleTasks();

            // Since the queue is empty, we can only wait for new tasks.
            try {
                synchronized( queue ){
                    if( isFinished() ){
                        break;
                    }
                    if( Settings.traceMasterProgress || Settings.traceWaits ){
                        if( everyoneBusy ) {
                            System.out.println( "Master: all workers busy; waiting");
                        }
                        else {
                            System.out.println( "Master: no work to distribute; waiting" );
                        }
                    }
                    queue.wait();
                }
            } catch (InterruptedException e) {
                // Not interested.
            }
        }
        stopTime = System.nanoTime();
        System.out.println( "End of master thread" );
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    void removeIbis( IbisIdentifier theIbis )
    {
	synchronized( queue ) {
	    ArrayList<TaskInstance> orphans = workers.removeWorker( theIbis );
	    queue.add( orphans );
	}
    }

    /** Returns the identifier of (the receive port of) this worker.
     * 
     * @return The identifier.
     */
    ReceivePortIdentifier identifier()
    {
        return receivePort.identifier();
    }

    /** Print some statistics about the entire master run. */
    void printStatistics( PrintStream s )
    {
        if( stopTime<startTime ) {
            System.err.println( "Worker didn't stop yet" );
        }
        queue.printStatistics( s );
        long workInterval = stopTime-startTime;
        s.printf(  "Master: # workers        = %5d\n", workers.size() );
        s.printf(  "Master: # incoming tasks = %5d\n", incomingTaskCount );
        s.printf(  "Master: # handled tasks  = %5d\n", handledTaskCount );
        s.println( "Master: run time         = " + Service.formatNanoseconds( workInterval ) );
        sendPort.printStats( s, "master send port" );
        workers.printStatistics( s );
    }

    CompletionInfo[] getCompletionInfo( JobList jobs )
    {
        synchronized( queue ) {
            return queue.getCompletionInfo( jobs, workers );
        }
    }

    /** This ibis is suspect; don't try to talk to it for the moment. */
    void setSuspect( IbisIdentifier ibisIdentifier )
    {
	synchronized( queue ){
	    workers.setSuspect( ibisIdentifier );
	}
    }

    /** This ibis is unsuspect. */
    void setUnsuspect( IbisIdentifier ibisIdentifier )
    {
        synchronized( queue ){
            workers.setUnsuspect( ibisIdentifier );
        }
    }
}
