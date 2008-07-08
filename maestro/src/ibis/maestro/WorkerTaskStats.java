package ibis.maestro;

import java.io.PrintStream;

class WorkerTaskStats {
    private int taskCount = 0;
    private long workDuration = 0;        
    private long queueDuration = 0;     // Cumulative queue time of all tasks.
    final TimeEstimate workTime = new TimeEstimate( Service.MILLISECOND_IN_NANOSECONDS );
    final TimeEstimate queueTimePerTask = new TimeEstimate( Service.MILLISECOND_IN_NANOSECONDS );

    /**
     * Registers the completion of a task of this particular type, with the
     * given queue interval and the given work interval.
     * @param queueInterval The time this task spent in the queue.
     * @param workInterval The time it took to execute this task.
     */
    void countTask( long queueInterval, long workInterval )
    {
        taskCount++;
        queueDuration += queueInterval;
        workDuration += workInterval;
    }

    void reportStats( PrintStream out, TaskType t, double workInterval )
    {
        double workPercentage = 100.0*(workDuration/workInterval);
        if( taskCount>0 ) {
            out.println( "Worker: " + t + ":" );
            out.printf( "    # tasks          = %5d\n", taskCount );
            out.println( "    total work time = " + Service.formatNanoseconds( workDuration ) + String.format( " (%.1f%%)", workPercentage )  );
            out.println( "    queue time/task  = " + Service.formatNanoseconds( queueDuration/taskCount ) );
            out.println( "    work time/task   = " + Service.formatNanoseconds( workDuration/taskCount ) );
            out.println( "    aver. dwell time = " + Service.formatNanoseconds( (workDuration+queueDuration)/taskCount ) );
        }
        else {
            out.println( "Worker: " + t + " is unused" );
        }
    }

    /**
     * @param queueLength The current length of the work queue for this type.
     * @return The estimated current dwell time on this worker for this task.
     */
    long getEstimatedDwellTime( int queueLength )
    {
        return workTime.getAverage() + queueTimePerTask.getAverage()*queueLength;
    }

    /**
     * Update the estimate for the queue time per task.
     * @param v The new value for the queue time per task.
     */
    public void setQueueTimePerTask( long v )
    {
        queueTimePerTask.addSample( v );
    }
}