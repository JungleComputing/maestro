package ibis.maestro;

/**
 * This class summarizes the information about the best worker for the
 * next job.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class WorkerSelector {
    WorkerInfo bestWorker;
    long bestSubmissionInterval;

    WorkerSelector()
    {
	reset();
    }

    /** Resets the worker selector. */
    public void reset() {
	bestWorker = null;
	bestSubmissionInterval = Long.MAX_VALUE;
    }
}
