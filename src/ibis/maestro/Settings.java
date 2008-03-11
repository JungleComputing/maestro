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
    public static final int OPTIONAL_COMMUNICATION_TIMEOUT = 60000;
    
    /** Message transmission timeout in ms on essential communications. */
    public static final int ESSENTIAL_COMMUNICATION_TIMEOUT = 5*OPTIONAL_COMMUNICATION_TIMEOUT;

    /** The number of connections we maximally keep open. */
    public static final int CONNECTION_CACHE_SIZE = 8;

    // Debugging flags.
    
    /** Trace the progress of workers? */
    public static final boolean traceWorkerProgress = true;
    
    /** Trace the handling or result jobs? */
    public static final boolean traceResultJobs = false;

    /** Trace the progress of the worker lists of masters. */
    public static final boolean traceWorkerList = false;
    
    /** Trace the creation and destruction of Nodes. */
    public static final boolean traceNodes = false;

    /** Trace the selection of the fastest worker. */
    public static final boolean traceFastestWorker = false;

    /** Trace the progress of masters? */
    public static final boolean traceMasterProgress = false;

    /** Trace the adventures of the precompletion interval variable? */
    public static final boolean traceSubmissionInterval = false;

    /** Trace all send events. */
    public static final boolean traceSends = false;
}
