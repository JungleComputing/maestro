package ibis.maestro;

import java.io.Serializable;

/**
 * A completed job.
 * 
 * @author Kees van Reeuwijk.
 */
class CompletedJob {
    final JobInstanceIdentifier job;

    final Serializable result;

    /**
     * Constructs a new CompletedJob.
     * 
     * @param job
     *            The identifier of the job that was completed.
     * @param result
     *            The result of the completed job.
     */
    CompletedJob(JobInstanceIdentifier job, Serializable result) {
        this.job = job;
        this.result = result;
    }

}
