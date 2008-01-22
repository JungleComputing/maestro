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
     * Produce a log file with information for a timing trace?
     */
    public static final boolean writeTrace = true;
}
