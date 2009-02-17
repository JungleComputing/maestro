package ibis.maestro;

/**
 * A message containing the result of an entire job.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class JobResultMessage extends Message {
    private static final long serialVersionUID = 5158569253342276404L;

    final JobInstanceIdentifier job;

    final Object result;

    /**
     * Constructs a new result message.
     * 
     * @param job
     *            The identifier of the job this is a result for.
     * @param result
     *            The result value.
     */
    protected JobResultMessage(JobInstanceIdentifier job, Object result) {
        this.job = job;
        this.result = result;
    }

}
