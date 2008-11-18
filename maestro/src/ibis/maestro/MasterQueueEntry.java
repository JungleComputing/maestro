/**
 * 
 */
package ibis.maestro;

final class MasterQueueEntry {
    // FIXME: remove this container class.
    final TaskInstance task;

    /**
     * @param task
     * @param antTrail
     */
    MasterQueueEntry( TaskInstance task )
    {
        this.task = task;
    }

    String shortLabel() 
    {
	return task.shortLabel();
    }
}