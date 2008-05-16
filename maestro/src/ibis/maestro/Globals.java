package ibis.maestro;


/**
 * Global variables.
 * 
 * @author Kees van Reeuwijk
 *
 */
class Globals {
    /** The name of the master port of a node. */
    static final String masterReceivePortName = "masterReceivePort";
    
    /** The name of the worker port of a node. */
    static final String workerReceivePortName = "workerReceivePort";
    
    /** The logger. */
    final static Logger log = new Logger();
}
