package ibis.maestro;

import java.util.ArrayList;
import java.util.HashMap;

import junit.framework.Assert;

/**
 * The list of all known jobs of this run.
 * 
 * @author Kees van Reeuwijk.
 */
public final class JobList {
    // A map from each job to its job type.
    private final HashMap<Job, JobType> jobTypeMap = new HashMap<Job, JobType>();

    // A map from each type index to the job it belongs to.
    private final ArrayList<Job> indexToJobMap = new ArrayList<Job>();

    // A list of all known types.
    private final ArrayList<JobType> allJobTypes = new ArrayList<JobType>();

    // For each job type, the list types to do.
    private final ArrayList<JobType[]> todoLists = new ArrayList<JobType[]>();

    /**
     * Register a new job sequence.
     * 
     * @param job
     *            The job to register.
     * @return The type of the registered job.
     */
    public JobType registerJob(Job job) {
        if (jobTypeMap.containsKey(job)) {
            // No need to register, we already have it.
            return jobTypeMap.get(job);
        }
        JobType t;
        int index;
        if (job instanceof UnpredictableAtomicJob) {
            index = allJobTypes.size();
            t = new JobType(true, true, index);
            todoLists.add(new JobType[] { t });
        } else if (job instanceof AtomicJob) {
            index = allJobTypes.size();
            t = new JobType(false, true, index);
            todoLists.add(new JobType[] { t });
        } else if (job instanceof SeriesJob) {
            final SeriesJob sjob = (SeriesJob) job;
            final Job jobs[] = sjob.jobs;

            boolean unpredictable = false;
            final ArrayList<JobType> todoList = new ArrayList<JobType>();
            for (final Job j : jobs) {
                final JobType jobType = registerJob(j);
                unpredictable |= jobType.unpredictable;
                final JobType tl1[] = getTodoList(jobType);

                for (final JobType e : tl1) {
                    todoList.add(e);
                }
            }
            index = allJobTypes.size();
            t = new JobType(unpredictable, false, index);
            final JobType todoArray[] = todoList
            .toArray(new JobType[todoList.size()]);
            todoLists.add(todoArray);
        } else if (job instanceof ParallelJob) {
            final boolean unpredictable = true; // FIXME: allow predictable parallel jobs.
            index = allJobTypes.size();
            t = new JobType(unpredictable, false, index);
            todoLists.add(new JobType[] { t });
        } else {
            Globals.log.reportError("Don't know how to register job type "
                    + job.getClass());
            t = null;
        }
        jobTypeMap.put(job, t);
        allJobTypes.add(t);
        indexToJobMap.add(job);
        return t;
    }

    JobType getJobType(Job job) {
        return jobTypeMap.get(job);
    }

    Job getJob(JobType type) {
        return indexToJobMap.get(type.index);
    }

    JobType[] getTodoList(JobType type) {
        return todoLists.get(type.index);
    }

    JobType[] getTodoList(Job job) {
        final JobType jobType = getJobType(job);

        return getTodoList(jobType);
    }

    JobType[] getAllTypes() {
        return allJobTypes.toArray(new JobType[allJobTypes.size()]);
    }

    /**
     * Given a a job, return the initial estimate for its execution time.
     * 
     * @param job
     *            The job we want the initial estimate for.
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
        if (job instanceof AlternativesJob) {
            // We estimate this will be the minimum of all alternatives.
            final AlternativesJob aj = (AlternativesJob) job;
            double time = Double.POSITIVE_INFINITY;

            for (final Job j : aj.alternatives) {
                final double t1 = initialEstimateJobTime(j);
                if (t1 < time) {
                    time = t1;
                }
            }
            return time;
        }
        if (job instanceof SeriesJob) {
            final SeriesJob l = (SeriesJob) job;
            double time = 0.0;

            for (final Job j : l.jobs) {
                double t1 = initialEstimateJobTime(j);

                if (t1 == Double.POSITIVE_INFINITY) {
                    /*
                     * Yes, this looks weird, but infinity here means we cannot
                     * execute the job locally. We must assume that it can be
                     * executed remotely, but we don't know the execution time
                     * there.
                     */
                    t1 = 0.0;
                }
                time += t1;
            }
            return time;
        }
        return 0.0;
    }

    double[] getInitialJobTimes() {
        final double res[] = new double[allJobTypes.size()];
        int i = 0;
        for (final JobType t : allJobTypes) {
            final Job job = getJob(t);
            final double time = initialEstimateJobTime(job);
            res[i++] = time;
        }
        return res;
    }

    /**
     * @return Return the number of registered types.
     */
    int getTypeCount() {
        return allJobTypes.size();
    }

    JobType getStageType(JobType type, int i) {
        final JobType todoList[] = getTodoList(type);
        return todoList[i];
    }

    boolean isParallelJobType(JobType t) {
        Job job = indexToJobMap.get(t.index);
        return job instanceof ParallelJob;
    }

    /** Do some sanity checks on the administration. */
    void sanityCheck() {
        final int size = allJobTypes.size();

        Assert.assertEquals(todoLists.size(), size);
        Assert.assertEquals(indexToJobMap.size(), size);
        Assert.assertEquals(jobTypeMap.size(), size);
    }
}
