package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A simple class to store a task, worker pair.
 * 
 * @author Kees van Reeuwijk
 *
 */
class Submission {
    final TaskInstance task;
    final IbisIdentifier worker;
    final long predictedDuration;

    /**
     * @param task
     * @param worker
     * @param predictedDuration
     */
    Submission(TaskInstance task, IbisIdentifier worker,
	    long predictedDuration) {
	this.task = task;
	this.worker = worker;
	this.predictedDuration = predictedDuration;
    }
}
