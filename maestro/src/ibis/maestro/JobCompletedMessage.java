package ibis.maestro;

/**
 * A message from the worker to the master, telling the master that the worker
 * has completed the given job.
 * 
 * @author Kees van Reeuwijk
 */
final class JobCompletedMessage extends Message {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** The identifier of the job. */
    final long jobId;

    /**
     * Constructs a job-completed message for the master of a job.
     * 
     * @param jobId
     *            The identifier of the job, as handed out by the master.
     */
    JobCompletedMessage(final long jobId) {
        this.jobId = jobId;
    }

    /**
     * Returns a string representation of this status message.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "job completed message: jobId=" + jobId;
    }
}
