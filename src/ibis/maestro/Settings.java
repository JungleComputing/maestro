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
    public static final int OPTIONAL_COMMUNICATION_TIMEOUT = 40000;
    
    /** Messate transmission timeout in ms on essential communications. */
    public static final int ESSENTIAL_COMMUNICATION_TIMEOUT = 60000;

    // Debugging flags.
    
    /**
     * Trace the progress of workers?
     */
    public static final boolean traceWorkerProgress = true;

    /**
     * Trace the progress of the worker lists of masters.
     */
    public static final boolean traceWorkerList = true;
    
    /**
     * Trace the creation and destruction of Nodes.
     */
    public static final boolean traceNodes = true;

    /**
     * Trace the selection of the fastest worker.
     */
    public static final boolean traceFastestWorker = true;

    /**
     * Trace the progress of masters?
     */
    public static final boolean traceMasterProgress = true;

    /** Trace the adventures of the precompletion interval variable? */
    public static final boolean traceSubmissionInterval = true;

    /** Trace all send events. */
    public static final boolean traceSends = false;
}
