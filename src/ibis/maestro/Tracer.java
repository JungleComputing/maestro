package ibis.maestro;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * Handle logging events.
 *
 * @author Kees van Reeuwijk
 */
public class Tracer {

    private final ObjectOutputStream stream;

    /** Create a new logger. */
    Tracer( File file )
    {
	stream = openStream( file );
    }

    private ObjectOutputStream openStream(File file) {
	OutputStream fis;
	try {
	    fis = new FileOutputStream( file );
	    return new ObjectOutputStream( fis );
	} catch (IOException e) {
	    System.err.println( "Cannot open trace file '" + file + "': " + e.getLocalizedMessage() );
	}
	return null;
    }

    /** Write a message to the log file. */
    void log( TraceEvent e )
    {
	try {
	    stream.writeObject( e );
	} catch (IOException x ) {
	    System.err.println( "Cannot write to trace file" );
	}
    }
    
    /** Close the logger. */
    public void close()
    {
	if( stream != null ) {
	    try {
		stream.close();
	    } catch (IOException e) {
		System.err.println( "Cannot close trace file" );
	    }
	}
    }

    /**
     * Given a message that has been sent, write an entry to the trace file.
     * @param msg The message that was sent.
     */
    public void traceSentMessage(Message msg )
    {
	TraceEvent e = msg.buildTraceEvent( true );
	log( e );
    }


    /**
     * Given a message that has been received, write an entry to the trace file.
     * @param msg The message that was received.
     */
    public void traceReceivedMessage(Message msg )
    {
	TraceEvent e = msg.buildTraceEvent( false );
	log( e );
    }
}
