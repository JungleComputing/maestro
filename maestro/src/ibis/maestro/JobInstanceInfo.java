package ibis.maestro;

class JobInstanceInfo {
    final JobInstanceIdentifier identifier;

    final JobSequence job;

    final JobCompletionListener listener;

    double startTime = Utils.getPreciseTime();

    final JobInstance taskInstance;

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
            JobInstance taskInstance, JobSequence job,
            final JobCompletionListener listener) {
        this.identifier = identifier;
        this.taskInstance = taskInstance;
        this.job = job;
        this.listener = listener;
    }
}