/**
 * 
 */
package ibis.maestro;

import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisIdentifier;

import java.io.IOException;
import java.io.PrintStream;

/**
 * A node using ant routing.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class AntNode extends Node
{
    private AntRoutingTable antRoutingTable = new AntRoutingTable();

    /**
     * Constructs a new Maestro node using the given list of jobs. Optionally
     * try to get elected as maestro.
     * @param jobs The jobs that should be supported in this node.
     * @param runForMaestro If true, try to get elected as maestro.
     * @throws IbisCreationFailedException Thrown if for some reason we cannot create an ibis.
     * @throws IOException Thrown if for some reason we cannot communicate.
     */
    public AntNode(JobList jobs, boolean runForMaestro) throws IbisCreationFailedException, IOException
    {
        super( jobs, runForMaestro );
    }

    /**
     * Given a number of nodes to wait for, keep waiting until we have gossip information about
     * at least this many nodes, or until the given time has elapsed.
     * @param n The number of nodes to wait for.
     * @param maximalWaitTime The maximal time in ms to wait for these nodes.
     * @return The actual number of nodes there was information for at the moment we stopped waiting.
     */
    public int waitForReadyNodes( int n, long maximalWaitTime )
    {
        // FIXME: implement waitForReadyNodes for ant routing.
        return 0;
    }

    protected void failNode( RunTaskMessage message, Throwable t )
    {
        super.failNode( message, t );
    }

    protected RunTaskMessage getWork()
    {
        return workerQueue.remove( null );        
    }

    /**
     * Given an input and a list of possible jobs to execute, submit
     * this input as a job with the best promised completion time.
     * If <code>submitIfBusy</code> is set, also consider jobs where all
     * workers are currently busy.
     * @param input The input of the job.
     * @param submitIfBusy If set, also consider jobs for which all workers are currently busy.
     * @param listener The completion listener for this job.
     * @param choices The list of job choices.
     * @return <code>true</code> if the job could be submitted.
     */
    boolean submit( Object input, boolean submitIfBusy, JobCompletionListener listener, Job...choices )
    {
        int choice;

        if( choices.length == 0 ){
            // No choices? Obviously we won't be able to submit this one.
            return false;
        }
        if( choices.length == 1 && submitIfBusy ){
            choice = 0;
        }
        else {
            // FIXME: do a smarter selection of the job choices for ant routing.
            choice = 0;
        }
        Job job = choices[choice];
        job.submit( this, input, job, listener );
        return true;
    }

    private Counter updateMessageCount = new Counter();

    /** On a locked queue, try to send out as many tasks as we can. */
    protected void drainMasterQueue()
    {
        while( true ) {
            NodeInfo worker;
            long taskId;
            IbisIdentifier node;
            TaskInstance task;
    
            synchronized( antRoutingTable ) {
                Submission submission = masterQueue.getAntSubmission( antRoutingTable );
                if( submission == null ) {
                    break;
                }
                node = submission.worker;
                task = submission.task;
                worker = nodes.get( node );
                taskId = nextTaskId++;
    
                worker.registerTaskStart( task, taskId, submission.predictedDuration );
            }
            if( Settings.traceMasterQueue || Settings.traceSubmissions ) {
                Globals.log.reportProgress( "Submitting task " + task + " to " + node );
            }
            RunTaskMessage msg = new RunTaskMessage( node, task, taskId );
            boolean ok = sendPort.send( node, msg );
            if( ok ){
                submitMessageCount.add();
            }
            else {
                // Try to put the paste back in the tube.
                // The send port has already registered the trouble.
                masterQueue.add( msg.taskInstance );
                worker.retractTask( taskId );
            }
        }
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * @param m The update message.
     */
    protected void handleNodeUpdateMessage( UpdateNodeMessage m )
    {
        if( Settings.traceNodeProgress ){
            Globals.log.reportProgress( "Received node update message " + m );
        }
        handleNodeUpdateInfo( m.update );
    }

    protected void updateRecentMasters()
    {
    }

    /** Print some statistics about the entire worker run. */
    synchronized void printStatistics( PrintStream s )
    {
        super.printStatistics( s );
        s.printf( "update        messages:   %5d sent\n", updateMessageCount.get() );
    }

    /**
     * @param message The task that was run.
     * @param result The result of the task.
     * @param runMoment The moment the task was started.
     */
    void handleTaskResult(RunTaskMessage message, Object result, long runMoment) {
        long taskCompletionMoment = System.nanoTime();
    
        TaskType type = message.taskInstance.type;
        taskResultMessageCount.add();
    
        TaskType nextTaskType = jobs.getNextTaskType( type );
        if( nextTaskType == null ){
            // This was the final step. Report back the result.
            JobInstanceIdentifier identifier = message.taskInstance.jobInstance;
            boolean ok = sendJobResultMessage( identifier, result );
            if( !ok ) {
        	// Could not send the result message. We're in trouble.
        	// Just try again.
        	ok = sendJobResultMessage(identifier, result );
        	if( !ok ) {
        	    // Nothing we can do, we give up.
        	    Globals.log.reportError( "Could not send job result message to " + identifier );
        	}
            }
        }
        else {
            // There is a next step to take.
            TaskInstance nextTask = new TaskInstance( message.taskInstance.jobInstance, nextTaskType, result );
            submit( nextTask );
        }
    
        runningTasks.down();
        if( Settings.traceNodeProgress || Settings.traceRemainingJobTime ) {
            long queueInterval = runMoment-message.getQueueMoment();
            Globals.log.reportProgress( "Completed " + message.taskInstance + "; queueInterval=" + Utils.formatNanoseconds( queueInterval ) + "; runningTasks=" + runningTasks );
        }
        long workerDwellTime = taskCompletionMoment-message.getQueueMoment();
        if( traceStats ) {
            double now = 1e-9*(System.nanoTime()-startTime);
            System.out.println( "TRACE:workerDwellTime " + type + " " + now + " " + 1e-9*workerDwellTime );
        }
        Message msg = new TaskCompletedMessage( message.taskId, workerDwellTime );
        boolean ok = sendPort.send( message.source, msg );
    
        if( !ok ) {
            // Could not send the result message. We're desperate.
            // First simply try again.
            ok = sendPort.send( message.source, msg );
            if( !ok ) {
        	// Unfortunately, that didn't work.
        	// TODO: think up another way to recover from failed result report.
        	Globals.log.reportError( "Failed to send task completed message to " + message.source );
            }
        }
    }

    /**
     * Handle a message containing a new task to run.
     * 
     * @param msg The message to handle.
     */
    protected void handleRunTaskMessage( RunTaskMessage msg )
    {
        IbisIdentifier source = msg.source;
        nodes.registerAsCommunicating( source );
        workerQueue.add( msg );
    }

    /**
     * A node has sent us a gossip message, handle it.
     * @param m The gossip message.
     */
    protected void handleGossipMessage(GossipMessage m)
    {
    }
}
