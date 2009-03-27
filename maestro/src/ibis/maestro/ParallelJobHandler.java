package ibis.maestro;

import java.io.Serializable;

/**
 * Handles the merge phase of parallel (split/merge) jobs. In particular, it
 * acts as the JobCompletionListener for the sub-jobs of a parallel job.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class ParallelJobHandler implements JobCompletionListener {
    private final Node localNode;

    /**
     * @param localNode
     *            The node we are running on.
     */
    ParallelJobHandler(Node localNode) {
        this.localNode = localNode;
    }

    /**
     * The identifier of a job instance for our and the user's administration.
     * 
     * @author Kees van Reeuwijk
     *
     */
    private static final class Id implements Serializable {
        private static final long serialVersionUID = 1L;

        final Serializable userID;

        final ParallelJobInstance instance;

        private Id(final Serializable userID, final ParallelJobInstance jobInstance) {
            super();
            this.userID = userID;
            this.instance = jobInstance;
        }

        /**
         * @return A string representation of this id.
         */
        @Override
        public String toString() {
            return "jobInstance=" + instance + " userID=" + userID;
        }
    }

    /**
     * Submits a new job instance with the given input.
     * Internally we keep track of the number of submitted jobs so that we can
     * wait for all of them to complete.
     * 
     * @param input
     *            Input for the job.
     * @param jobInstance
     *            The parallel job instance this submission belongs to.
     * @param userId
     *            The identifier the user attaches to this job.
     * @param job
     *            The job to submit to.
     */
    @SuppressWarnings("synthetic-access")
    public synchronized void submit(Serializable input, ParallelJobInstance jobInstance, Serializable userId,
            Job job) {
        final Serializable id = new Id(userId, jobInstance);
        if (Settings.traceParallelJobs) {
            Globals.log.reportProgress("ParallelJobHandler: Submitting " + id + " to "
                    + job);
        }
        localNode.submitAlways(input, id, this,job);
    }

    /**
     * Handle the completion of a job. We do this by storing the result in a
     * local array.
     * 
     * @param node
     *            The node we're running on.
     * @param userId
     *            The identifier of the job that was completed.
     * @param result
     *            The result of the job.
     */
    @Override
    public synchronized void jobCompleted(Node node, Object userId,
            Serializable result) {
        if (Settings.traceParallelJobs) {
            Globals.log.reportProgress("ParallelJobHandler: got back " + userId);
        }
        if (!(userId instanceof Id)) {
            Globals.log
            .reportInternalError("The identifier is not a ParallelJobHandler.Id but a "
                    + userId.getClass());
            return;
        }
        final Id id = (Id) userId;
        final ParallelJobInstance instance = id.instance;

        instance.merge(id.userID, result);

        if( instance.resultIsReady() ){
            final Serializable mergedResult = instance.getResult();
            instance.handleJobResult(node, mergedResult);
        }
    }
}
