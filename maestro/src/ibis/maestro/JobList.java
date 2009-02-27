package ibis.maestro;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;

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

    void printStatistics(@SuppressWarnings("unused") PrintStream s) {
    }

    /**
     * Register a new job sequence.
     * 
     * @param job
     *            The job to register.
     */
    JobType registerJob(Job job) {
        if( jobTypeMap.containsKey(job)) {
            // No need to register, we already have it.
            return jobTypeMap.get(job);
        }
        JobType t;
        int index = allJobTypes.size();
        indexToJobMap.add(job);
        if( job instanceof UnpredictableAtomicJob ) {
            t = new JobType( true, index );
            todoLists.add( new JobType[] { t } );
        }
        else if( job instanceof AtomicJob ) {
            t = new JobType( false, index );
            todoLists.add( new JobType[] { t } );
        }
        else if( job instanceof SeriesJob ) {
            SeriesJob sjob = (SeriesJob) job;
            final Job jobs[] = sjob.jobs;
            ArrayList<JobType> todoList = new ArrayList<JobType>();

            boolean unpredictable = false;
            int i = jobs.length;
            while( i>0 ) {
                i--;

                final Job t1 = jobs[i];
                final JobType jobType = registerJob( t1 );
                unpredictable |= jobType.unpredictable;
                JobType tl1[] = getTodoList(t1);
                for( JobType e: tl1) {
                    todoList.add( e );
                }
            }
            t = new JobType( unpredictable, index );
            JobType todoArray[] = todoList.toArray(new JobType[todoList.size()]);
            todoLists.add( todoArray );
        }
        else {
            Globals.log.reportError( "Don't know how to register job type " + job.getClass() );
            t = null;
        }
        jobTypeMap.put(job, t);
        allJobTypes.add(t);
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
        JobType jobType = jobTypeMap.get(job);
        return getTodoList(jobType);
    }

    JobType[] getAllTypes() {
        return allJobTypes.toArray(new JobType[allJobTypes.size()]);
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

    /**
     * @return Return the number of registered types.
     */
    int getTypeCount() {
        return allJobTypes.size();
    }

    JobType getStageType(JobType type, int i) {
        JobType todoList[] = getTodoList(type);
        return todoList[i];
    }
}
