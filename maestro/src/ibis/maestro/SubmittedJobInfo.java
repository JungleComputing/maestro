package ibis.maestro;

/**
 * Some information about a running submitted job instance. This class is used
 * by nodes to keep track of running job instances that were submitted
 * by the user to the Maestro system.
 * 
 * @author Kees van Reeuwijk
 *
 */
class SubmittedJobInfo {
    final JobInstanceIdentifier identifier;

    final JobCompletionListener listener;

    double startTime = Utils.getPreciseTime();

    final JobInstance jobInstance;

    final boolean restart;

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
    SubmittedJobInfo(final JobInstanceIdentifier identifier,
            JobInstance jobInstance,
            final JobCompletionListener listener,boolean restart) {
        this.identifier = identifier;
        this.jobInstance = jobInstance;
        this.listener = listener;
        this.restart = restart;
    }
}