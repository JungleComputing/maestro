package ibis.maestro;

import ibis.maestro.LabelTracker.Label;

import java.io.Serializable;

/**
 * Handles the execution of a split/merge job. In particular, it handles the
 * submission of sub-jobs, waits for their completion, merges the results, and
 * retreives the result.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class ParallelJobHandler extends Thread implements JobCompletionListener {
    private final Node localNode;

    private final ParallelJob reducer;

    private final LabelTracker labeler = new LabelTracker();

    private final RunJobMessage message;

    private final double runMoment;

    /**
     * @param localNode
     *            The node we are running on.
     * @param reducer
     *            The reducer to run on each result we're waiting for.
     */
    ParallelJobHandler(Node localNode, ParallelJob reducer,
            RunJobMessage message, double runMoment) {
        this.localNode = localNode;
        this.reducer = reducer;
        this.message = message;
        this.runMoment = runMoment;
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

        final Label label;

        private Id(final Serializable userID, final Label label) {
            super();
            this.userID = userID;
            this.label = label;
        }

        /**
         * @return A string representation of this id.
         */
        @Override
        public String toString() {
            return "label=" + label + " userID=" + userID;
        }
    }

    /**
     * Submits a new job instance with the given input.
     * Internally we keep track of the number of submitted jobs so that we can
     * wait for all of them to complete.
     * 
     * @param input
     *            Input for the job.
     * @param userId
     *            The identifier the user attaches to this job.
     * @param job
     *            The job to submit to.
     */
    @SuppressWarnings("synthetic-access")
    public synchronized void submit(Object input, Serializable userId,
            Job job) {
        Label label = labeler.nextLabel();
        Serializable id = new Id(userId, label);
        if (Settings.traceParallelJobs) {
            Globals.log.reportProgress("ParallelJobHandler: Submitting " + id + " to "
                    + job);
        }
        localNode.submit(input, id, this,job);
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
            Object result) {
        if (Settings.traceParallelJobs) {
            Globals.log.reportProgress("ParallelJobHandler: got back " + userId);
        }
        if (!(userId instanceof Id)) {
            Globals.log
                    .reportInternalError("The identifier is not a ParallelJobHandler.Id but a "
                            + userId.getClass());
            return;
        }
        Id id = (Id) userId;
        labeler.returnLabel(id.label);
        reducer.merge(id.userID, result);
    }

    /**
     * Runs this thread. We assume that the split phase has been completed, so
     * all we have to do is wait for the return of all results. The user should
     * not use this method.
     */
    @Override
    public void run() {
        try {
            labeler.waitForAllLabels();
        } catch (InterruptedException x) {
            // Nothing we can do.
        }
        Object result = reducer.getResult();
        localNode.handleJobResult(message, result, runMoment);
    }

}
