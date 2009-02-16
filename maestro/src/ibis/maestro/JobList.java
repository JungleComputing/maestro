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
    private final ArrayList<JobSequence> jobSequences = new ArrayList<JobSequence>();

    private final ArrayList<JobType> allJobTypes = new ArrayList<JobType>();

    private final ArrayList<JobType> supportedJobTypes = new ArrayList<JobType>();

    private int jobCounter = 0;

    /**
     * Add a new job to this list.
     * 
     * @param job The job to add.
     */
    void add(JobSequence job) {
        jobSequences.add(job);
    }

    JobSequence get(int i) {
        return jobSequences.get(i);
    }

    int size() {
        return jobSequences.size();
    }

    private JobSequence searchJobID(JobSequence.JobSequenceIdentifier id) {
        for (final JobSequence t : jobSequences) {
            if (t.id.equals(id)) {
                return t;
            }
        }
        return null;
    }

    JobType getPreviousTaskType(JobType t) {
        final JobSequence job = searchJobID(t.job);
        if (job == null) {
            Globals.log
                    .reportInternalError("getPreviousTaskType(): task type with unknown job id: "
                            + t);
            return null;
        }
        return job.getPreviousTaskType(t);
    }

    void printStatistics(PrintStream s) {
        for (final JobSequence t : jobSequences) {
            t.printStatistics(s);
        }
    }

    /**
     * Register a new job sequence.
     * 
     * @param job
     *            The job to register.
     */
    void registerJob(JobSequence job) {
        final Job tasks[] = job.jobs;

        for (int i = 0; i < tasks.length; i++) {
            final Job t = tasks[i];

            final JobType taskType = job.jobTypes[i];
            if (t.isSupported()) {
                if (Settings.traceTypeHandling) {
                    Globals.log.reportProgress("Node supports task type "
                            + taskType);
                }
                supportedJobTypes.add(taskType);
            }
            final int ix = taskType.index;
            while (allJobTypes.size() <= ix) {
                allJobTypes.add(null);
            }
            if (allJobTypes.get(ix) != null) {
                Globals.log.reportInternalError("Duplicate type index " + ix);
            }
            allJobTypes.set(ix, taskType);
        }
    }

    /**
     * Creates a job with the given name and the given sequence of tasks. The
     * jobs in the task will be executed in the given order.
     * 
     * @param jobs
     *            The list of tasks of the job.
     * @return A new job instance representing this job.
     */
    public JobSequence createJobSequence(Job... jobs) {
        final int jobId = jobCounter++;
        final JobSequence job = new JobSequence(jobId, jobs);

        jobSequences.add(job);
        registerJob(job);
        return job;
    }

    /**
     * Returns a list of all the supported task types.
     * 
     * @return A list of all supported task types.
     */
    JobType[] getSupportedJobTypes() {
        return supportedJobTypes.toArray(new JobType[supportedJobTypes
                .size()]);
    }

    JobSequence findJob(JobType type) {
        return jobSequences.get(type.job.id);
    }

    Job getJob(JobType type) {
        final JobSequence job = findJob(type);
        final Job task = job.jobs[type.taskNo];
        return task;
    }

    JobType getNextJobType(JobType type) {
        final JobSequence job = findJob(type);
        return job.getNextTaskType(type);
    }

    int getNumberOfTaskTypes() {
        return jobCounter;
    }

    JobType[] getAllTypes() {
        return allJobTypes.toArray(new JobType[allJobTypes.size()]);
    }

    /**
     * Given the index of a type, return the next one in the job, or -1 if there
     * isn't one.
     * 
     * @param ix
     *            The index of a type.
     * @return The index of the next type.
     */
    int getNextIndex(int ix) {
        final JobType type = allJobTypes.get(ix);
        if (type == null) {
            return -1;
        }
        final JobType nextType = getNextJobType(type);
        if (nextType == null) {
            return -1;
        }
        return nextType.index;
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
        for (final JobSequence job : jobSequences) {
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
        if( job instanceof JobSequence ){
            JobSequence l = (JobSequence) job;
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
