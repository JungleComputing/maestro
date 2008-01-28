package ibis.maestro;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.PriorityQueue;

/** Process trace files to generate a coherent time trace.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class ProcessTraces {
    private static PriorityQueue<TraceEvent> events = new PriorityQueue<TraceEvent>();

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
	while( !events.isEmpty() ) {
	    TraceEvent e = events.poll();
	    e.print();
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
