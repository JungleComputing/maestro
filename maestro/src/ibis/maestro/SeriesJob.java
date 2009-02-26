package ibis.maestro;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A job sequence.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public final class SeriesJob implements Job {
    final Job jobs[];

    /** Returns true iff this job type is supported.
     * Always returns true since job sequences are always supported.
     * @return <code>true</code> since job sequences are always supported.
     */
    @Override
    public boolean isSupported()
    {
        return true;
    }

    SeriesJob(final Job... jobs) {
        this.jobs = jobs;
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
    private void submit(Node node, Object value, Serializable userId,
            JobCompletionListener listener) {
        final JobInstanceIdentifier tii = buildJobInstanceIdentifier(userId);
        final JobType type = null; // FIXME: needs right job type
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
        return "SeriesJob [" + Arrays.deepToString(jobs) + "]";
    }
}
