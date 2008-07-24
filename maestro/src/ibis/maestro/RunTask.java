package ibis.maestro;

/**
 * A task to run for a work thread.
 * @author Kees van Reeuwijk
 *
 */
class RunTask {
    final AtomicTask task;
    RunTaskMessage message;

    RunTask( final AtomicTask task, final RunTaskMessage message ) {
        this.task = task;
        this.message = message;
    }

}
