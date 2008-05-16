package ibis.maestro;

/**
 * Settings, such as debugging flags, for the Maestro software.
 * 
 * @author Kees van Reeuwijk
 *
 */
class Settings {
    // Timeout values.
    
    /** Message transmission timeout in ms on optional communication. */
    static final int OPTIONAL_COMMUNICATION_TIMEOUT = 30000;
    
    /** Message transmission timeout in ms on essential communications. */
    static final int ESSENTIAL_COMMUNICATION_TIMEOUT = 5*OPTIONAL_COMMUNICATION_TIMEOUT;

    /** The number of connections we maximally keep open. */
    static final int CONNECTION_CACHE_SIZE = 80;

    // Debugging flags.
    
    /** Trace type handling? */
    static final boolean traceTypeHandling = false;

    /** Trace the progress of workers? */
    static final boolean traceWorkerProgress = false;

    /** Trace the handling or result jobs? */
    static final boolean traceResultJobs = false;

    /** Trace the progress of the worker lists of masters. */
    static final boolean traceWorkerList = false;
    
    /** Trace the creation and destruction of Nodes. */
    static final boolean traceNodes = false;

    /** Trace the events in the master queue? */
    static final boolean traceMasterQueue = false;

    /** Trace the administration of remaining task time. */
    static final boolean traceRemainingTaskTime = false;

    /** Trace the progress of masters? */
    static final boolean traceMasterProgress = false;

    /** Trace all send events. */
    static final boolean traceSends = false;

    /** We limit the master queue to this many jobs per known worker. */
    static final int JOBS_PER_WORKER = 10;
}