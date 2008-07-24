package ibis.maestro;

import java.io.PrintStream;

class WorkerTaskStats {
    final TaskType type;
    private int taskCount = 0;
    private long totalWorkTime = 0;        
    private long totalQueueTime = 0;     // Cumulative queue time of all tasks.
    final TimeEstimate averageWorkTime = new TimeEstimate( Service.MILLISECOND_IN_NANOSECONDS );
    final TimeEstimate queueTimePerTask = new TimeEstimate( Service.MILLISECOND_IN_NANOSECONDS );

    WorkerTaskStats( TaskType type )
    {
	this.type = type;
    }

    /**
     * Registers the completion of a task of this particular type, with the
     * given queue interval and the given work interval.
     * @param queueTime The time this task spent in the queue.
     * @param workTime The time it took to execute this task.
     */
    void countTask( long queueTime, long workTime )
    {
        taskCount++;
        totalQueueTime += queueTime;
        totalWorkTime += workTime;
        averageWorkTime.addSample( workTime );
    }

    void reportStats( PrintStream out, double workTime )
    {
        double workPercentage = 100.0*(totalWorkTime/workTime);
        if( taskCount>0 ) {
            out.println( "Worker: " + type + ":" );
            out.printf( "    # tasks          = %5d\n", taskCount );
            out.println( "    total work time = " + Service.formatNanoseconds( totalWorkTime ) + String.format( " (%.1f%%)", workPercentage )  );
            out.println( "    queue time/task  = " + Service.formatNanoseconds( totalQueueTime/taskCount ) );
            out.println( "    work time/task   = " + Service.formatNanoseconds( totalWorkTime/taskCount ) );
            out.println( "    aver. dwell time = " + Service.formatNanoseconds( (totalWorkTime+totalQueueTime)/taskCount ) );
        }
        else {
            out.println( "Worker: " + type + " is unused" );
        }
    }

    /**
     * @param queueLength The current length of the work queue for this type.
     * @return The estimated current dwell time on this worker for this task.
     */
    long getEstimatedDwellTime( int queueLength )
    {
        long res = averageWorkTime.getAverage() + queueTimePerTask.getAverage()*queueLength;
        return res;
    }

    /**
     * Update the estimate for the queue time per task.
     * @param v The new value for the queue time per task.
     */
    public void setQueueTimePerTask( long v )
    {
        queueTimePerTask.addSample( v );
    }

    /** Returns the estimated time to compute this task.
     * @return The estimated time.
     */
    long getEstimatedComputeTime()
    {
        return averageWorkTime.getAverage();
    }
}
