package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A simple class to store a task, worker pair.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class Submission {
    final JobInstance job;

    final IbisIdentifier worker;

    final double predictedDuration;

    /**
     * @param task
     * @param worker
     * @param predictedDuration
     */
    Submission(JobInstance task, IbisIdentifier worker, double predictedDuration) {
        this.job = task;
        this.worker = worker;
        this.predictedDuration = predictedDuration;
    }
}
