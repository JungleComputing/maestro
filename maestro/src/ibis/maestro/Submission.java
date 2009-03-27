package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A simple class to store a job, worker pair.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class Submission {
    final JobInstance jobInstance;

    final IbisIdentifier worker;

    final double predictedDuration;

    /**
     * @param job
     * @param worker
     * @param predictedDuration
     */
    Submission(JobInstance job, IbisIdentifier worker, double predictedDuration) {
        this.jobInstance = job;
        this.worker = worker;
        this.predictedDuration = predictedDuration;
    }
}
