package ibis.maestro;

import java.io.PrintStream;

/**
 * The information for one type of task in the queue.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class WorkerQueueType extends QueueType<RunTaskMessage> {

    WorkerQueueType( TaskType type ){
	super( type );
    }
    
    void printStatistics( PrintStream s )
    {
        printStatistics( s, "worker" );
    }
}