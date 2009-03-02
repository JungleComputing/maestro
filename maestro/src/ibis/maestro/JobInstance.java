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

    /** The type of job we are executing. */
    final JobType type;

    /** The type of the current stage. */
    final JobType stageType;

    /** The index in the todo list of this job type. */
    final int stage;

    private boolean orphan = false;

    /**
     * @param tii
     *            The identifier of this job instance.
     * @param type
     *            The type of this job instance.
     * @param input
     *            The input for this job.
     * @param type
     *            The overall type of job to execute
     * @param stageType
     *            The type of the current stage of the job
     * @param stage
     *            The index in the todo list of the current state of the job
     */
    JobInstance(JobInstanceIdentifier tii, Object input, JobType type,
            JobType stageType, int stage) {
        this.jobInstance = tii;
        this.input = input;
        this.type = type;
        this.stageType = stageType;
        this.stage = stage;
        if (!stageType.isAtomic) {
            Globals.log.reportInternalError("Non-atomic stage type "
                    + stageType);
        }
    }

    String formatJobAndType() {
        return "(jobId=" + jobInstance.id + ",type=" + type + "stage=" + stage
                + ")";
    }

    /**
     * Returns a string representation of this job instance.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(job instance: job instance=" + jobInstance + " type=" + type
                + " stage=" + stage + " input=" + input + ")";
    }

    String shortLabel() {
        return jobInstance.label() + "#" + type + "@" + stage;
    }

    void setOrphan() {
        orphan = true;
    }

    boolean isOrphan() {
        return orphan;
    }
}
