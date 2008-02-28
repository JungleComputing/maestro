package ibis.maestro;

/**
 * Settings, such as debugging flags, for the Maestro software.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Settings {
    // Timeout values.
    
    /** Work request timeout in ms on optional communication. */
    public static final int OPTIONAL_COMMUNICATION_TIMEOUT = 4000;
    
    /** Messate transmission timeout in ms on essential communications. */
    public static final int ESSENTIAL_COMMUNICATION_TIMEOUT = 60000;

    // Debugging flags.
    
    /**
     * Trace the progress of workers?
     */
    public static final boolean traceWorkerProgress = false;

    /**
     * Trace the progress of the worker lists of masters.
     */
    public static final boolean traceWorkerList = false;
    
    /**
     * Trace the creation and destruction of Nodes.
     */
    public static final boolean traceNodes = false;

    /**
     * Produce a log file with information for a timing trace?
     */
    public static final boolean writeTrace = true;

    /**
     * Trace the selection of the fastest worker.
     */
    public static final boolean traceFastestWorker = false;

    /**
     * Trace the progress of masters?
     */
    public static final boolean traceMasterProgress = false;

    /** Trace the adventures of the precompletion interval variable? */
    public static final boolean traceSubmissionInterval = false;

    /** Trace all send events. */
    public static final boolean traceSends = false;
}
