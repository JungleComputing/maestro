package ibis.maestro;

/**
 * Some information about a running job instance. This class is used
 * by nodes to keep track of running job instances.
 * 
 * @author Kees van Reeuwijk
 *
 */
class JobInstanceInfo {
    final JobInstanceIdentifier identifier;

    final JobCompletionListener listener;

    double startTime = Utils.getPreciseTime();

    final JobInstance jobInstance;

    /**
     * Constructs an information class for the given job identifier.
     * 
     * @param identifier
     *            The job identifier.
     * @param job
     *            The job this belongs to.
     * @param listener
     *            The completion listener associated with the job.
     */
    JobInstanceInfo(final JobInstanceIdentifier identifier,
            JobInstance jobInstance,
            final JobCompletionListener listener) {
        this.identifier = identifier;
        this.jobInstance = jobInstance;
        this.listener = listener;
    }
}