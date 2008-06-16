package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;

/**
 * A message from a worker to a master, telling it that we can handle
 * the given types of job.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RegisterTypeMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    
    final CompletionInfo completionInfo[];

    /** The list of types we support. */
    final JobType allowedType[];

    /**
     * Constructs a new type registration request message.
     * @param identifier Our identifier with this master.
     * @param completionInfo The completion times from each job type to the end of the task.
     * @param allowedTypes Which types of jobs can it handle?
     */
    RegisterTypeMessage( WorkerIdentifier identifier, CompletionInfo[] completionInfo, JobType allowedType[] ){
	super( identifier );
	this.completionInfo = completionInfo;
	this.allowedType = allowedType;
    }
}
