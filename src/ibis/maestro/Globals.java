package ibis.maestro;

import java.io.File;

/**
 * Global variables.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Globals {
    /** The name of the master port of a node. */
    public static final String masterReceivePortName = "masterReceivePort";
    
    /** The name of the worker port of a node. */
    public static final String workerReceivePortName = "workerReceivePort";
    
    /** The logger. */
    public final static Logger log = new Logger();
    
    /** The event tracer. */
    public final static Tracer tracer = new Tracer( new File( "tracefile" ) );
}
