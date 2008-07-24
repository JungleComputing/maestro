package ibis.maestro;

/**
 * A task to run for a work thread.
 * @author Kees van Reeuwijk
 *
 */
class RunTask {
    final Task task;
    final RunTaskMessage message;

    RunTask( final Task task, final RunTaskMessage message ) {
        this.task = task;
        this.message = message;
    }

}
