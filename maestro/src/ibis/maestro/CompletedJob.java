package ibis.maestro;

/**
 * A completed job.
 * 
 * @author Kees van Reeuwijk.
 */
class CompletedJob {
    final JobInstanceIdentifier job;

    final Object result;

    /**
     * Constructs a new CompletedJob.
     * 
     * @param job
     * @param result
     */
    CompletedJob(JobInstanceIdentifier job, Object result) {
        this.job = job;
        this.result = result;
    }

}
