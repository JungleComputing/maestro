package ibis.maestro;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A list of completed jobs.
 * Used as a buffer to asynchroneously report results back to the submitter.
 * 
 * @author Kees van Reeuwijk
 *
 */
class CompletedJobList extends ConcurrentLinkedQueue<CompletedJob> {
    private static final long serialVersionUID = 1L;
}
