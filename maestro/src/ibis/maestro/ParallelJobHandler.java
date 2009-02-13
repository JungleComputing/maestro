package ibis.maestro;

import ibis.maestro.LabelTracker.Label;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Handles the execution of a split/merge job. In particular, it handles the
 * submission of sub-tasks, waits for their completion, merges the results, and
 * retreives the result.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class ParallelJobHandler extends Thread implements JobCompletionListener {
    final Node localNode;

    final ParallelJob reducer;

    final LabelTracker labeler = new LabelTracker();

    final RunJobMessage message;

    final double runMoment;

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

    private static final class Id implements Serializable {
        private static final long serialVersionUID = 1L;

        final Object userID;

        final Label label;

        private Id(final Object userID, final Label label) {
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
     * Submits a new job instance with the given input for the first task of the
     * job. Maestro is free to pick one of the given list of alternative jobs.
     * Internally we keep track of the number of submitted jobs so that we can
     * wait for all of them to complete.
     * 
     * @param input
     *            Input for the job.
     * @param userId
     *            The identifier the user attaches to this job.
     * @param submitIfBusy
     *            If <code>true</code> the job is submitted even if all
     *            workers are currently busy.
     * @param jobChoices
     *            The list of possible jobs to submit to.
     * @return <code>true</code> iff the job instance was submitted.
     */
    @SuppressWarnings("synthetic-access")
    public synchronized boolean submit(Object input, Object userId,
            boolean submitIfBusy, JobSequence... jobChoices) {
        Label label = labeler.nextLabel();
        Serializable id = new Id(userId, label);
        if (Settings.traceMapReduce) {
            Globals.log.reportProgress("MapReduce: Submitting " + id + " to "
                    + Arrays.deepToString(jobChoices));
        }
        boolean submitted = localNode.submit(input, id, submitIfBusy, this,
                jobChoices);
        if (!submitted) {
            // Not submitted, return this label to the administration.
            labeler.returnLabel(label);
        }
        return submitted;
    }

    /**
     * Handle the completion of a task. We do this by storing the result in a
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
        if (Settings.traceMapReduce) {
            Globals.log.reportProgress("MapReduce: got back " + userId);
        }
        if (!(userId instanceof Id)) {
            Globals.log
                    .reportInternalError("The identifier is not a MapReduceHandler.Id but a "
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
