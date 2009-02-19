package ibis.maestro;

import java.io.PrintStream;
import java.util.ArrayList;

/**
 * The list of all known jobs of this run.
 * 
 * @author Kees van Reeuwijk.
 */
public final class JobList {
    // FIXME: let the JobList store Jobs instead of JobSequences
    private final ArrayList<SeriesJob> jobSequences = new ArrayList<SeriesJob>();

    private final ArrayList<JobType> allJobTypes = new ArrayList<JobType>();

    private int jobCounter = 0;

    void printStatistics(@SuppressWarnings("unused") PrintStream s) {
        for (@SuppressWarnings("unused") final Job t : jobSequences) {
            // TODO: re-implement statistics printing.
        }
    }

    /**
     * Register a new job sequence.
     * 
     * @param job
     *            The job to register.
     */
    private void registerJob(SeriesJob job) {
        final Job jobs[] = job.jobs;

        for (int i = 0; i < jobs.length; i++) {
            final Job t = jobs[i];

            final JobType jobType = job.jobTypes[i];
            if (t.isSupported()) {
                if (Settings.traceTypeHandling) {
                    Globals.log.reportProgress("Node supports job type "
                            + jobType);
                }
            }
            final int ix = jobType.index;
            while (allJobTypes.size() <= ix) {
                allJobTypes.add(null);
            }
            if (allJobTypes.get(ix) != null) {
                Globals.log.reportInternalError("Duplicate type index " + ix);
            }
            allJobTypes.set(ix, jobType);
        }
    }

    /**
     * Creates a job with the given name and the given sequence of jobs. The
     * jobs in the sequence will be executed in the given order.
     * 
     * @param jobs
     *            The list of jobs of the job.
     * @return A new job instance representing this job.
     */
    public SeriesJob createSeriesJob(Job... jobs) {
        final int jobId = jobCounter++;
        final SeriesJob job = new SeriesJob(jobId, jobs);

        jobSequences.add(job);
        registerJob(job);
        return job;
    }

    Job getJob(JobType type) {
        final SeriesJob job = jobSequences.get(type.job.id);
        return job.jobs[type.jobNo];
    }

    JobType getNextJobType(JobType type) {
        final SeriesJob job = jobSequences.get(type.job.id);
        return job.getNextJobType(type);
    }

    JobType[] getAllTypes() {
        return allJobTypes.toArray(new JobType[allJobTypes.size()]);
    }

    /**
     * Returns an array of arrays with type indices that should be updated in
     * the given order from front to back.
     * 
     * @return
     */
    int[][] getIndexLists() {
        final int res[][] = new int[jobSequences.size()][];
        int jobno = 0;
        for (final SeriesJob job : jobSequences) {
            res[jobno++] = job.updateIndices;
        }
        return res;
    }

    double[] getInitialJobTimes() {
        final double res[] = new double[allJobTypes.size()];
        int i = 0;
        for (final JobType t : allJobTypes) {
            final Job job = getJob(t);
            double time = initialEstimateJobTime(job);
            res[i++] = time;
        }
        return res;
    }

    /**
     * Given a a job, return the initial estimate for its execution time.
     * @param job The job we want the initial estimate for.
     * @return The initial estimate of the execution time of this job.
     */
    private double initialEstimateJobTime(final Job job) {
        if (!job.isSupported()) {
            // Not supported by this node.
            return Double.POSITIVE_INFINITY;
        }
        if (job instanceof JobExecutionTimeEstimator) {
            final JobExecutionTimeEstimator estimator = (JobExecutionTimeEstimator) job;
            return estimator.estimateJobExecutionTime();
        }
        if (job instanceof AlternativesJob){
            // We estimate this will be the minimum of all alternatives.
            AlternativesJob aj = (AlternativesJob) job;
            double time = Double.POSITIVE_INFINITY;
            
            for( Job j: aj.alternatives){
                double t1 = initialEstimateJobTime( j );
                if( t1<time ){
                    time = t1;
                }
            }
            return time;
        }
        if( job instanceof SeriesJob ){
            SeriesJob l = (SeriesJob) job;
            double time = 0.0;

            for( Job j: l.jobs ){
                double t1 = initialEstimateJobTime( j );
                
                if( t1 == Double.POSITIVE_INFINITY ){
                    /* Yes, this looks weird, but infinity here
                     * means we cannot execute the job locally. We
                     * must assume that it can be executed remotely,
                     * but we don't know the execution time there.
                     */
                    t1 = 0.0;
                }
                time += t1;
            }
            return time;
        }
        return 0.0;
    }
}
