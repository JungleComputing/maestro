package ibis.maestro;

import java.io.Serializable;

/**
 * A job result as communicated from the worker to the master.
 * @author Kees van Reeuwijk
 *
 */
class JobResult<T> implements Serializable {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final T result;
    private long id;

    T getResult() { return result; }
    long getId() { return id; }
    
    JobResult( T result, long id ){
        this.result = result;
        this.id = id;
    }
}
