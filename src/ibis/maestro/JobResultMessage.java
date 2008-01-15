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
    private long id;

    JobReturn getResult() { return result; }
    long getId() { return id; }
    
    JobResultMessage( JobReturn r, long id ){
        this.result = r;
        this.id = id;
    }
}
