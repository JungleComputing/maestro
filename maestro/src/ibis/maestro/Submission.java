package ibis.maestro;

/**
 * A simple class to store a task, worker pair.
 * 
 * @author Kees van Reeuwijk
 *
 */
class Subtask {
    TaskInstance task = null;
    WorkerInfo worker = null;
    long deadline = 0L;
}
