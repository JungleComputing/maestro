package ibis.maestro;

/**
 * Settings, such as debugging flags, for the Maestro software.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Settings {
    // Timeout values.
    
    /** Message transmission timeout in ms on optional communication. */
    public static final int OPTIONAL_COMMUNICATION_TIMEOUT = 30000;
    
    /** Message transmission timeout in ms on essential communications. */
    public static final int ESSENTIAL_COMMUNICATION_TIMEOUT = 5*OPTIONAL_COMMUNICATION_TIMEOUT;

    /** The number of connections we maximally keep open. */
    public static final int CONNECTION_CACHE_SIZE = 80;

    // Debugging flags.
    
    /** Trace type handling? */
    public static final boolean traceTypeHandling = true;

    /** Trace the progress of workers? */
    public static final boolean traceWorkerProgress = true;

    /** Trace the handling or result jobs? */
    public static final boolean traceResultJobs = true;

    /** Trace the progress of the worker lists of masters. */
    public static final boolean traceWorkerList = false;
    
    /** Trace the creation and destruction of Nodes. */
    public static final boolean traceNodes = true;

    /** Trace the events in the master queue? */
    public static final boolean traceMasterQueue = true;

    /** Trace the administration of remaining task time. */
    public static final boolean traceRemainingTaskTime = true;

    /** Trace the progress of masters? */
    public static final boolean traceMasterProgress = true;

    /** Trace the adventures of the submission interval variable? */
    public static final boolean traceSubmissionInterval = true;

    /** Trace all send events. */
    public static final boolean traceSends = true;

    /** We limit the master queue to this many jobs per known worker. */
    public static final int JOBS_PER_WORKER = 10;
}
