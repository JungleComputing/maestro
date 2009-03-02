package ibis.maestro;

import java.util.Arrays;

/**
 * A job sequence.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class SeriesJob implements Job {
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

    public SeriesJob(final Job... jobs) {
        this.jobs = jobs;
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
