package ibis.maestro;

/**
 * Tell the worker to execute the job contained in this message.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class RunJobMessage extends Message {
    /** */
    private static final long serialVersionUID = 1L;

    final JobInstance jobInstance;

    final long jobId;

    /**
     * Given a job and its source, constructs a new RunJobMessage.
     * 
     * @param jobInstance
     *            The job to run.
     * @param jobId
     *            The identifier of the job.
     * @param todoList
     *            The list of subsequent jobs to do.
     */
    RunJobMessage(final JobInstance jobInstance, final long jobId) {
        this.jobInstance = jobInstance;
        this.jobId = jobId;
    }

    /**
     * Returns a string representation of this job message.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "Job message for job " + jobId + " of type " + jobInstance;
    }

    String label() {
        return jobInstance.shortLabel();
    }
}
