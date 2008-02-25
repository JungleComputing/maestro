package ibis.maestro;

/**
 * The available methods for a running job.
 * @author Kees van Reeuwijk
 *
 */
public interface JobContext {

    /**
     * Submit a new job from this job.
     * @param submitter The job that submitted this job.
     * @param job The job to submit.
     */
    void submit( Job submitter, Job job );

    /**
     * Report the given result.
     * @param receiver A receiver for job results.
     * @param value The result value to report.
     */
    void reportResult( ReportReceiver receiver, JobProgressValue value );

}
