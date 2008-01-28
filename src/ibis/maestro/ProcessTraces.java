package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.PriorityQueue;

/** Process trace files to generate a coherent time trace.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class ProcessTraces {
    private static PriorityQueue<TraceEvent> events = new PriorityQueue<TraceEvent>();
    private static int sourceNo = 0;
    private static HashMap<ReceivePortIdentifier, Integer> sourceMap = new HashMap<ReceivePortIdentifier,Integer>();

    private static void registerEvent( TraceEvent e )
    {
        ReceivePortIdentifier src = e.source;
        
        if( !sourceMap.containsKey( src ) ){
            sourceMap.put( src, sourceNo++ );
        }
    }
    
    private int getSourceID( ReceivePortIdentifier src )
    {
        return sourceMap.get( src );
    }

    private static void readFile( String fnm )
    {
	try {
	    InputStream fis = new FileInputStream( fnm );
	    ObjectInputStream in = new ObjectInputStream( fis );
	    while( true ) {
		TraceEvent res = (TraceEvent) in.readObject();
		if( res == null ) {
		    break;
		}
		events.add( res );
                registerEvent( res );
	    }
	    in.close();
	}
	catch( IOException e ){
	    System.err.println( "Cannot load trace file " + fnm + ": " + e.getLocalizedMessage() );
	}
	catch( ClassNotFoundException e ){
	    System.err.println( "Cannot load trace file " + fnm + ": " + e.getLocalizedMessage() );
	}		

    }

    private static void printEvents()
    {
        long startTime = 0L;
	while( !events.isEmpty() ) {
	    TraceEvent e = events.poll();
	    e.print( startTime, sourceMap );
	}
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
	for( String fnm: args ) {
	    readFile( fnm );
	}
	printEvents();
    }

}
