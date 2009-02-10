package ibis.maestro;

import java.io.PrintStream;
import java.util.ArrayList;

/**
 * The list of all known jobs of this run.
 * 
 * @author Kees van Reeuwijk.
 */
public final class JobList {
    private final ArrayList<JobSequence> jobs = new ArrayList<JobSequence>();

    private final ArrayList<JobType> allTaskTypes = new ArrayList<JobType>();

    private final ArrayList<JobType> supportedTaskTypes = new ArrayList<JobType>();

    private int jobCounter = 0;

    /**
     * Add a new jobs to this list.
     * 
     * @param job
     */
    void add(JobSequence job) {
        jobs.add(job);
    }

    JobSequence get(int i) {
        return jobs.get(i);
    }

    int size() {
        return jobs.size();
    }

    private JobSequence searchJobID(JobSequence.JobSequenceIdentifier id) {
        for (final JobSequence t : jobs) {
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
        for (final JobSequence t : jobs) {
            t.printStatistics(s);
        }
    }

    /**
     * Register a new job.
     * 
     * @param job
     *            The job to register.
     */
    void registerJob(JobSequence job) {
        final Job tasks[] = job.tasks;

        for (int i = 0; i < tasks.length; i++) {
            final Job t = tasks[i];

            final JobType taskType = job.jobTypes[i];
            if (t.isSupported()) {
                if (Settings.traceTypeHandling) {
                    Globals.log.reportProgress("Node supports task type "
                            + taskType);
                }
                supportedTaskTypes.add(taskType);
            }
            final int ix = taskType.index;
            while (allTaskTypes.size() <= ix) {
                allTaskTypes.add(null);
            }
            if (allTaskTypes.get(ix) != null) {
                Globals.log.reportInternalError("Duplicate type index " + ix);
            }
            allTaskTypes.set(ix, taskType);
        }
    }

    /**
     * Creates a job with the given name and the given sequence of tasks. The
     * jobs in the task will be executed in the given order.
     * 
     * @param name
     *            The name of the job.
     * @param tasks
     *            The list of tasks of the job.
     * @return A new job instance representing this job.
     */
    public JobSequence createJob(String name, Job... tasks) {
        final int jobId = jobCounter++;
        final JobSequence job = new JobSequence(jobId, name, tasks);

        jobs.add(job);
        registerJob(job);
        return job;
    }

    /**
     * Returns a list of all the supported task types.
     * 
     * @return A list of all supported task types.
     */
    JobType[] getSupportedTaskTypes() {
        return supportedTaskTypes.toArray(new JobType[supportedTaskTypes
                .size()]);
    }

    JobSequence findJob(JobType type) {
        return jobs.get(type.job.id);
    }

    Job getTask(JobType type) {
        final JobSequence job = findJob(type);
        final Job task = job.tasks[type.taskNo];
        return task;
    }

    JobType getNextTaskType(JobType type) {
        final JobSequence job = findJob(type);
        return job.getNextTaskType(type);
    }

    int getNumberOfTaskTypes() {
        return jobCounter;
    }

    JobType[] getAllTypes() {
        return allTaskTypes.toArray(new JobType[allTaskTypes.size()]);
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
        final JobType type = allTaskTypes.get(ix);
        if (type == null) {
            return -1;
        }
        final JobType nextType = getNextTaskType(type);
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
        final int res[][] = new int[jobs.size()][];
        int jobno = 0;
        for (final JobSequence job : jobs) {
            res[jobno++] = job.updateIndices;
        }
        return res;
    }

    double[] getInitialTaskTimes() {
        final double res[] = new double[allTaskTypes.size()];
        int i = 0;
        for (final JobType t : allTaskTypes) {
            final Job task = getTask(t);
            if (!task.isSupported()) {
                // Not supported by this node.
                res[i++] = Double.POSITIVE_INFINITY;
            } else if (task instanceof TaskExecutionTimeEstimator) {
                final TaskExecutionTimeEstimator estimator = (TaskExecutionTimeEstimator) task;
                res[i++] = estimator.estimateTaskExecutionTime();
            } else {
                res[i++] = 0l;
            }
        }
        return res;
    }
}
