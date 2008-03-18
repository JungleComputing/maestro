package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;

import java.util.AbstractList;
import java.util.ArrayList;

/**
 * A class representing the master work queue.
 * This requires a special implementation because we want to enforce
 * priorities for the different job types, and we want to know
 * which job types are currently present in the queue.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class MasterQueue {
    private final AbstractList<QueueEntry> queue = new ArrayList<QueueEntry>();

    private static final class QueueEntry {
        final Job job;
        final TaskIdentifier id;

        QueueEntry(Job job, TaskIdentifier id) {
            this.job = job;
            this.id = id;
        }
    }

    void submit( Job j, TaskIdentifier taskId )
    {
        QueueEntry e = new QueueEntry( j, taskId );
        queue.add( e );
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }

    int size()
    {
        return queue.size();
    }

    void incrementAllowance( WorkerIdentifier workerID, WorkerList workers )
    {
        // We already know that this worker can handle this type of
        // job, but he asks for a larger allowance.
        // We only increase it if at the moment there is a job of this
        // type in the queue.
        //
        // FIXME: Walking the queue and testing the type of each job is highly inefficient.
        for( QueueEntry e: queue ){
            JobType jobType = e.job.getType();

            // We're in need of a worker for this type of job; try to 
            // increase the allowance of this worker.
            if( workers.incrementAllowance( workerID, jobType ) ) {
                break;
            }
        }
    }

    /**
     * Given a list of workers and a submission structure to fill,
     * try to select a job and a worker to execute the job.
     * If there are no jobs in the queue, return false.
     * If there are jobs in the queue, but no workers to execute the
     * jobs, set the worker field of the submission to <code>null</code>.
     *
     * FIXME: see if we can factor out the empty queue test.
     * 
     * @param sub The submission structure to fill.
     * @param workers The list of workers to choose from.
     * @return True iff there currently is no work.
     */
    boolean selectJob( Submission sub, WorkerList workers )
    {
        // This is a pretty big operation to do in one atomic
        // 'gulp'. TODO: see if we can break it up somehow.
        int ix = queue.size();
        int jobToRun = 0;
        WorkerInfo worker = null;
        boolean nowork = (ix == 0);
        while( ix>0 ) {
            ix--;

            QueueEntry job = queue.get( ix );
            JobType jobType = job.job.getType();
            worker = workers.selectBestWorker( jobType );
            if( worker != null ) {
                // We have a job that we can run.
                jobToRun = ix;
                break;
            }
        }
        if( worker == null ){
            sub.worker = null;
            sub.job = null;
        }
        else {
            final QueueEntry e = queue.remove( jobToRun );
            // We have a job and a worker. Submit the job.
            sub.job = e.job;
            sub.worker = worker;
            sub.taskId = e.id;
        }
        return nowork;
    }
}
