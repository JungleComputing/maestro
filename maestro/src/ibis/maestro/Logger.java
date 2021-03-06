package ibis.maestro;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Handle logging events.
 * 
 * @author Kees van Reeuwijk
 */
class Logger {
    private final PrintStream logfile;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    /** Create a new logger. */
    Logger() {
        logfile = System.out;
    }

    /** Write a message to the log file. */
    void log(String msg) {
        logfile.println(msg);
    }

    /**
     * Report to the user that some progress has been made.
     * 
     * @param msg
     *            The message to send to the user.
     */
    void reportProgress(String msg) {
        logfile.print(dateFormat.format(new Date()));
        logfile.print(' ');
        logfile.println(msg);
        logfile.flush();
    }

    /**
     * Given an error message, report an error.
     * 
     * @param msg
     *            The error message.
     */
    void reportError(String msg) {
        logfile.print(dateFormat.format(new Date()));
        logfile.print(" Error: ");
        logfile.println(msg);
    }

    /**
     * Given an error message, report an internal error.
     * 
     * @param msg
     *            The error message.
     */
    void reportInternalError(String msg) {
        logfile.print(dateFormat.format(new Date()));
        logfile.print(" Internal error: ");
        logfile.println(msg);
        Throwable t = new Throwable();
        t.printStackTrace(logfile);
    }

    /**
     * Returns the print stream of this logger.
     * 
     * @return The print stream.
     */
    PrintStream getPrintStream() {
        return logfile;
    }
}
