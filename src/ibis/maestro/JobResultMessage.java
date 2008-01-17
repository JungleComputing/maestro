package ibis.maestro;

/**
 * A job result as communicated from the worker to the master.
 * @author Kees van Reeuwijk
 *
 */
class JobResultMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final JobReturn result;
    private long id;   // The identifier of the job
    private long computeTime;  // The time it took from worker queue entry to job completion.

    JobReturn getResult() { return result; }
    long getId() { return id; }
    long getComputeTime() { return computeTime; }

    JobResultMessage( JobReturn r, long id, long computeTime ){
        this.result = r;
        this.id = id;
        this.computeTime = computeTime;
    }
}
