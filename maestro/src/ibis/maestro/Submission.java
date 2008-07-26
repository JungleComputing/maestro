package ibis.maestro;

/**
 * A simple class to store a task, worker pair.
 * 
 * @author Kees van Reeuwijk
 *
 */
class Submission {
    TaskInstance task = null;
    NodeTaskInfo worker = null;
    long predictedDuration = 0L;
}
