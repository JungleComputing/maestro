package ibis.maestro;

import java.io.PrintStream;

/**
 * Handle logging events.
 *
 * @author Kees van Reeuwijk
 */
class Logger {
    private final PrintStream logfile;
    
    /** Create a new logger. */
    Logger()
    {
        logfile = System.out;
    }

    /** Write a message to the log file. */
    void log( String msg )
    {
        logfile.println( msg );
    }

    /**
     * Report to the user that some progress has been made.
     * @param msg The message to send to the user.
     */
    void reportProgress( String msg )
    {
        logfile.println( msg );
    }
    
    /** Given an error message,
     * report an error.
     * @param msg The error message.
     */
    void reportError( String msg )
    {
	logfile.print( "Error: " );
	logfile.println( msg );
    }

    /** Given an error message, report an internal error.
     * @param msg The error message.
     */
    void reportInternalError( String msg )
    {
        logfile.print( "Internal error: " );
        logfile.println( msg );
    }
    
    /** Close the logger. */
    void close()
    {
	if( logfile != null ) {
	    logfile.close();
	}
    }

    /** Returns the print stream of this logger.
     * @return The print stream.
     */
    PrintStream getPrintStream()
    {
        return logfile;
    }
}
