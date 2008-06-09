package ibis.maestro;

/**
 * A job to run for a work thread.
 * @author Kees van Reeuwijk
 *
 */
class RunJob {
    final Job job;
    RunJobMessage message;

    public RunJob(final Job job, final RunJobMessage message ) {
        this.job = job;
        this.message = message;
    }

}
