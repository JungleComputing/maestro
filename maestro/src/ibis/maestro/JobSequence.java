package ibis.maestro;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * A job sequence.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public final class JobSequence implements Job {
    final JobSequenceIdentifier id;

    final String name;

    final Job[] tasks;

    final JobType[] jobTypes;

    final int updateIndices[];

    final TimeEstimate jobTime = new TimeEstimate(0);

    private static int index = 0;

    static final class JobSequenceIdentifier implements Serializable {
        private static final long serialVersionUID = -5895857432770361027L;

        final int id;

        private JobSequenceIdentifier(int id) {
            this.id = id;
        }

        /**
         * Returns the hash code of this job.
         * 
         * @return The hash code.
         */
        @Override
        public int hashCode() {
            return id;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
         */
        /**
         * Returns true iff the given object is a job identifier that is equal
         * to this one.
         * 
         * @param obj
         *            The object to compare to.
         * @return True iff this and the given object are equal.
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final JobSequenceIdentifier other = (JobSequenceIdentifier) obj;
            return (id == other.id);
        }

        /**
         * Returns a string representation of this job.
         * 
         * @return The string representation.
         */
        @Override
        public String toString() {
            return "J" + id;
        }
    }
    
    /** Returns true iff this job type is supported.
     * Always returns true since job sequences are always supported.
     * @return
     */
    @Override
    public boolean isSupported()
    {
        return true;
    }

    @SuppressWarnings("synthetic-access")
    JobSequence(final int id, final String name, final Job[] tasks) {
        this.id = new JobSequenceIdentifier(id);
        this.name = name;
        this.tasks = tasks;
        jobTypes = new JobType[tasks.length];
        updateIndices = new int[tasks.length];
        int i = tasks.length;
        boolean unpredictable = false;
        // Walk the list from back to front to allow
        // earlier tasks to be marked unpredictable if one
        // it or one of the following tasks is unpredictable.
        int updateIndex = 0;
        while (i > 0) {
            i--;
            if (tasks[i] instanceof UnpredictableAtomicTask) {
                unpredictable = true;
            }
            final int newIndex = index + i;
            jobTypes[i] = new JobType(this.id, i, (tasks.length - 1) - i,
                    unpredictable, newIndex);
            updateIndices[updateIndex++] = newIndex;
        }
        index += tasks.length;
    }

    /**
     * Builds a new identifier containing the given user identifier.
     * 
     * @param userIdentifier
     *            The user identifier to include in this identifier.
     * @return The newly constructed identifier.
     */
    private JobInstanceIdentifier buildJobInstanceIdentifier(
            Serializable userIdentifier) {
        return new JobInstanceIdentifier(userIdentifier, Globals.localIbis
                .identifier());
    }

    /**
     * Submits a job by giving a user-defined identifier, and the input value to
     * the first task of the job.
     * 
     * @param node
     *            The node this job should run on.
     * @param value
     *            The value to submit.
     * @param userId
     *            The identifier for the user of this job.
     * @param listener
     *            The listener that should be informed when this job is
     *            completed.
     * @param antTrail
     *            The initial ant trail to use for this job.
     */
    void submit(Node node, Object value, Serializable userId,
            JobCompletionListener listener, ArrayList<AntPoint> antTrail) {
        final JobInstanceIdentifier tii = buildJobInstanceIdentifier(userId);
        final JobType type = jobTypes[0];
        final JobInstance taskInstance = new JobInstance(tii, type, value,
                antTrail);
        node.addRunningJob(tii, taskInstance, this, listener);
        node.submit(taskInstance);
    }

    /**
     * Returns a string representation of this job.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(job " + name + " " + id + ")";
    }
    
    /**
     * Returns the name of this job.
     * @return The name.
     */
    @Override
    public String getName()
    {
        return toString();
    }

    /**
     * Given a task type, return the previous one in the task sequence of this
     * job, or <code>null</code> if there isn't one.
     * 
     * @param jobType
     *            The current job type.
     * @return The next job type, or <code>null</code> if there isn't one.
     */
    JobType getPreviousTaskType(JobType jobType) {
        if (!id.equals(jobType.job)) {
            Globals.log
                    .reportInternalError("getPreviousTaskType(): not my job: "
                            + jobType.job);
            return null;
        }
        if (jobType.taskNo > 0) {
            return jobTypes[jobType.taskNo - 1];
        }
        return null;
    }

    /**
     * Given a task type, return the next one in the task sequence of this job,
     * or <code>null</code> if there isn't one.
     * 
     * @param taskType
     *            The current task type.
     * @return The next task type, or <code>null</code> if there isn't one.
     */
    JobType getNextTaskType(JobType taskType) {
        if (!id.equals(taskType.job)) {
            Globals.log.reportInternalError("getNextTaskType(): not my job: "
                    + taskType.job);
            return null;
        }
        if (taskType.taskNo < tasks.length - 1) {
            return jobTypes[taskType.taskNo + 1];
        }
        return null;
    }

    void registerJobTime(double jobInterval) {
        // Changes are not interesting, since this is part of a big change
        // anyway.
        jobTime.addSample(jobInterval);
    }

    /**
     * Prints some statistics for this job.
     * 
     * @param s
     *            The stream to print to.
     */
    public void printStatistics(PrintStream s) {
        s.println("Job '" + name + "': " + jobTime.toString());
    }

    static int getTaskCount() {
        return index;
    }

    /**
     * Returns the first task type of this job.
     * 
     * @return The first task type.
     */
    JobType getFirstTaskType() {
        return jobTypes[0];
    }
}