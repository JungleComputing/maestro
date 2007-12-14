package ibis.maestro;

/**
 * A job result as communicated from the worker to the master.
 * @author Kees van Reeuwijk
 *
 */
class JobResult<T> {
    private final T result;
    private long id;

    T getResult() { return result; }
    long getId() { return id; }
    
    JobResult( T result, long id ){
        this.result = result;
        this.id = id;
    }
}
