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

    final Serializable input;

    /** The overall type of job we are executing. */
    final JobType overallType;

    /** The index in the todo list of this job type. */
    final int stageNumber;

    private boolean orphan = false;

    /**
     * @param jii
     *            The identifier of this job instance.
     * @param overallType
     *            The type of this job instance.
     * @param input
     *            The input for this job.
     * @param overallType
     *            The overall type of job to execute
     * @param stageNumber
     *            The index in the todo list of the current state of the job
     */
    JobInstance(JobInstanceIdentifier jii, Serializable input, JobType overallType,
            int stageNumber) {
        this.jobInstance = jii;
        this.input = input;
        this.overallType = overallType;
        this.stageNumber = stageNumber;
    }

    String formatJobAndType() {
        return "(jobId=" + Utils.deepToString(jobInstance.ids) + ",overallType=" + overallType + "stageNumber=" + stageNumber
        + ")";
    }

    /**
     * Returns a string representation of this job instance.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(job instance=" + jobInstance + " overallType=" + overallType
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

    JobType getStageType(JobList jobs) {
        return jobs.getStageType(overallType, stageNumber);
    }
}
