package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * Tell the worker to execute the job contained in this message.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class RunJobMessage extends Message {
    /** */
    private static final long serialVersionUID = 1L;

    final IbisIdentifier workerIdentifier;

    final JobInstance jobInstance;

    final long jobId;

    final IbisIdentifier source;

    /**
     * Given a job and its source, constructs a new RunJobMessage.
     * 
     * @param workIdentifier
     *            Who sent this job?
     * @param job
     *            The job to run.
     * @param jobId
     *            The identifier of the job.
     */
    RunJobMessage(IbisIdentifier workerIdentifier, JobInstance job,
            long jobId) {
        this.source = Globals.localIbis.identifier();
        this.workerIdentifier = workerIdentifier;
        this.jobInstance = job;
        this.jobId = jobId;
    }

    /**
     * Returns a string representation of this job message.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "Job message for job " + jobId + " of type "
                + jobInstance.type;
    }

    String label() {
        return jobInstance.shortLabel();
    }
}
