package ibis.maestro;

/**
 * Settings, such as debugging flags, for the Maestro software.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Settings {
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
}
