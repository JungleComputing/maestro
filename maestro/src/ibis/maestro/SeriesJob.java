package ibis.maestro;

import java.io.Serializable;

/**
 * A job sequence.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public final class SeriesJob implements Job {
    private final SeriesJobIdentifier id;

    final Job jobs[];

    final JobType jobTypes[];

    final int updateIndices[];

    private static int index = 0;

    static final class SeriesJobIdentifier implements Serializable {
        private static final long serialVersionUID = -5895857432770361027L;

        final int id;

        private SeriesJobIdentifier(int id) {
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
            final SeriesJobIdentifier other = (SeriesJobIdentifier) obj;
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
     * @return <code>true</code> since job sequences are always supported.
     */
    @Override
    public boolean isSupported()
    {
        return true;
    }

    @SuppressWarnings("synthetic-access")
    SeriesJob(final int id, final Job[] jobs) {
        this.id = new SeriesJobIdentifier(id);
        this.jobs = jobs;
        jobTypes = new JobType[jobs.length];
        updateIndices = new int[jobs.length];
        int i = jobs.length;
        boolean unpredictable = false;
        // Walk the list from back to front to allow
        // earlier jobs to be marked unpredictable if one
        // it or one of the following jobs is unpredictable.
        int updateIndex = 0;
        while (i > 0) {
            i--;
            if (jobs[i] instanceof UnpredictableAtomicJob) {
                unpredictable = true;
            }
            final int newIndex = index + i;
            jobTypes[i] = new JobType(this.id, i,
                    unpredictable, newIndex);
            updateIndices[updateIndex++] = newIndex;
        }
        index += jobs.length;
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
     * the first job of the job sequence.
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
     */
    void submit(Node node, Object value, Serializable userId,
            JobCompletionListener listener) {
        final JobInstanceIdentifier tii = buildJobInstanceIdentifier(userId);
        final JobType type = jobTypes[0];
        final JobInstance jobInstance = new JobInstance(tii, type, value);
        node.addRunningJob(tii, jobInstance, listener);
        node.submit(jobInstance);
    }

    /**
     * Returns a string representation of this job.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(job " + id + ")";
    }

    /**
     * Given a job type, return the next one in the job sequence of this job,
     * or <code>null</code> if there isn't one.
     * 
     * @param jobType
     *            The current job type.
     * @return The next job type, or <code>null</code> if there isn't one.
     */
    JobType getNextJobType(JobType jobType) {
        if (jobType.jobNo < jobs.length - 1) {
            return jobTypes[jobType.jobNo + 1];
        }
        return null;
    }
}
