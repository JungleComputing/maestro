package ibis.maestro;

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
    
    WorkerQueueInfo getWorkerQueueInfo( long dwellTime )
    {
        return new WorkerQueueInfo( type, queue.size(), dwellTime );
    }
}