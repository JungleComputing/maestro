package ibis.maestro;

/**
 * A simple class to store a job, worker pair.
 * 
 * @author Kees van Reeuwijk
 *
 */
class Submission {
    JobInstance job = null;
    WorkerInfo worker = null;
    TaskInstanceIdentifier taskId = null;
}