package ibis.maestro;

import java.io.Serializable;

/**
 * The representation of a job instance.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class JobInstance implements Serializable {
    private static final long serialVersionUID = -5669565112253289488L;

    final JobInstanceIdentifier jobInstance;

    final Object input;

    /** The overall type of job we are executing. */
    final JobType overallType;

    /** The type of the current stage. */
    final JobType stageType;

    /** The index in the todo list of this job type. */
    final int stageNumber;

    private boolean orphan = false;

    /**
     * @param tii
     *            The identifier of this job instance.
     * @param overallType
     *            The type of this job instance.
     * @param input
     *            The input for this job.
     * @param overallType
     *            The overall type of job to execute
     * @param stageType
     *            The type of the current stage of the job
     * @param stage
     *            The index in the todo list of the current state of the job
     */
    JobInstance(JobInstanceIdentifier tii, Object input, JobType overallType,
            JobType stageType, int stage) {
        this.jobInstance = tii;
        this.input = input;
        this.overallType = overallType;
        this.stageType = stageType;
        this.stageNumber = stage;
        if (!stageType.isAtomic) {
            Globals.log.reportInternalError("Non-atomic stage type "
                    + stageType);
        }
    }

    String formatJobAndType() {
        return "(jobId=" + jobInstance.id + ",overallType=" + overallType + "stageNumber=" + stageNumber
                + ")";
    }

    /**
     * Returns a string representation of this job instance.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(job instance: job instance=" + jobInstance + " overallType=" + overallType
                + " stageNumber=" + stageNumber + " input=" + input + ")";
    }

    String shortLabel() {
        return jobInstance.label() + "#" + overallType + "@" + stageNumber;
    }

    void setOrphan() {
        orphan = true;
    }

    boolean isOrphan() {
        return orphan;
    }
}
