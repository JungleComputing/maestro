package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

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

    private ObjectOutputStream openStream(File file)
    {
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
    synchronized void log( TraceEvent e )
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
		stream.writeObject( null );   // End-of-file marker
		stream.close();
	    } catch (IOException e) {
		System.err.println( "Cannot close trace file" );
	    }
	}
    }
    
    private long extractJobId( Message msg )
    {
        if( msg instanceof RunJobMessage ){
            return ((RunJobMessage) msg).jobId;
        }
        if( msg instanceof JobResultMessage ){
            return ((JobResultMessage) msg).jobId;
        }
        return 0L;
    }

    /**
     * Given a message that has been sent, write an entry to the trace file.
     * @param msg The message that was sent.
     * @param dest The destination of this message.
     */
    public void traceSentMessage(Message msg, ReceivePortIdentifier dest )
    {
	TraceEvent e = msg.buildSendTraceEvent( dest, extractJobId(msg) );
	log( e );
    }


    /**
     * Given a message that has been received, write an entry to the trace file.
     * @param msg The message that was received.
     * @param dest The destination of this message.
     */
    public void traceReceivedMessage(Message msg, ReceivePortIdentifier dest )
    {
	TraceEvent e = msg.buildReceiveTraceEvent( dest, extractJobId(msg) );
	log( e );
    }

    public void traceAlias( ReceivePortIdentifier a, ReceivePortIdentifier b )
    {
        log( new TraceAlias( System.nanoTime(), a, b ) );
    }
}
