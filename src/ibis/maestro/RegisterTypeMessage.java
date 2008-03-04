package ibis.maestro;

/**
 * A message from a worker to a master, telling it that we can handle
 * the given type of job.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RegisterTypeMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    
    /** The list of types we support. */
    final JobType allowedType;

    /**
     * Constructs a new type registration request message.
     * @param identifier Our identifier with this master.
     * @param allowedTypes Which types of jobs can it handle?
     */
    RegisterTypeMessage( int identifier, JobType allowedType ){
	super( identifier );
	this.allowedType = allowedType;
    }
}
