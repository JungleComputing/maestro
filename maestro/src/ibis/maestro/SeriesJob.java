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
     * Returns true iff the first stage is supported,
     * since all other elements can be submitted to other nodes.
     * @return <code>true</code> iff the first stage of the series is supported.
     */
    @Override
    public boolean isSupported()
    {
        return jobs.length == 0 || jobs[0].isSupported();
    }

    /**
     * Constructs a series job.
     * @param jobs The list of jobs in this series.
     */
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
