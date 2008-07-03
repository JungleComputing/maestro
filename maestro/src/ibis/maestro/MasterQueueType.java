package ibis.maestro;

import java.io.PrintStream;

/**
 * The information for one type of task in the queue.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class MasterQueueType extends QueueType<TaskInstance> {

    MasterQueueType( TaskType type ){
	super( type );
    }

    /**
     * Returns a string representation of this queue.. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "Q for " + type + "; " + queue.size() + " elm.; dQi=" + dequeueInterval; 
    }

    CompletionInfo getCompletionInfo( JobList jobs, WorkerList workers )
    {
        long averageCompletionTime = workers.getAverageCompletionTime( type );
        long duration;

        if( averageCompletionTime == Long.MAX_VALUE ) {
            duration = Long.MAX_VALUE;
        }
        else {
            long queueTime = estimateQueueTime();
            duration = queueTime + averageCompletionTime;
        }
        TaskType previousType = jobs.getPreviousTaskType( type );
        if( previousType == null ) {
            return null;
        }
        return new CompletionInfo( previousType, duration );
    }

    void printStatistics( PrintStream s )
    {
	printStatistics( s, "master" );
    }
}
