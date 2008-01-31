package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Vector;

/** Process trace files to generate a coherent time trace.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class ProcessTraces {
    private static PriorityQueue<TraceEvent> events = new PriorityQueue<TraceEvent>();
    private static int portNo = 0;
    private static long startTime = Long.MAX_VALUE;
    private static long endTime = Long.MIN_VALUE;
    private static double slotOffset = 50;
    private static double slotSeparation = 100;
    private static double textSeparation = 20;
    private static HashMap<ReceivePortIdentifier, Integer> portMap = new HashMap<ReceivePortIdentifier,Integer>();
    private static final Vector<Slot> slots = new Vector<Slot>();

    private static final double timeScale = 0.000003;

    private static class Slot {
        final long id;
        final long start;
        final String label;

        public Slot(final long id, final long start, final String label) {
            super();
            this.id = id;
            this.start = start;
            this.label = label;
        }
    }

    private static void registerPort( ReceivePortIdentifier p )
    {
	if( p != null && !portMap.containsKey( p ) ){
            portMap.put( p, portNo++ );
        }
    }

    private static void registerEvent( TraceEvent e )
    {
        long t = e.time;
        if( t<startTime ){
            startTime = t;
        }
        if( t>endTime ){
            endTime = t;
        }
        registerPort( e.source );
        registerPort( e.dest );
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
	    System.err.println( "Cannot load trace file '" + fnm + "': " + e.getLocalizedMessage() );
	}		

    }
    
    private static void printSVGBar( double x, double y, double length ){
        System.out.println( "<g><path" );
        System.out.println( "  style=\"stroke:#000000;stroke-width:1px\"" );
        System.out.println( "  d=\"M" + x + "," + y + " H" + (x+length) + "\"" );
        System.out.println( "/></g>" );
    }
    
    private static void printText( double x, double y, String t )
    {
        System.out.println( "<text" );
        System.out.println( "  xml:space=\"preserve\"" );
        System.out.println( "  style=\"font-size:12px;font-style-normal;font-weight:normal;font-family:Bitstream Vera Sans\"" );
        System.out.println( "  x=\"" + x + "\"" );
        System.out.println( "  y=\"" + y + "\"" );
        System.out.println( ">" );
        System.out.println( t );
        System.out.println( "</text>" );
    }

    private static void printEvents()
    {
	while( !events.isEmpty() ) {
	    TraceEvent e = events.poll();
	    e.print( startTime, portMap );
	}
    }
    
    /** Returns the index in the list of slots of the message with
     * the given id, or -1 if tehre is no such slot.
     * @param id The identifier of the slot.
     * @return The index in the list of slots, or -1 if not found.
     */
    private static int searchSlot( long id )
    {
        for( int i=0; i<slots.size(); i++ ){
            Slot s = slots.get(i);
            if( s != null && id == s.id ){
                return i;
            }
        }
        return -1;
    }
    
    /** Remove any empty slots at the end of this list. */
    private static void cleanSlots()
    {
        while( slots.size()>0 ){
            int ix = slots.size()-1;
            Slot s = slots.get(ix);
            if( s != null ){
                break;
            }
            slots.remove( ix );
        }
    }

    private static void printSVGEvent( TraceEvent e )
    {
       if( e.sent ){
           String lbl = e.getDescription(portMap);
           Slot s = new Slot( e.id, e.time, lbl );
           slots.add( s );
       }
       else {
           // A received event.
           int slotno = searchSlot( e.id );
           if( slotno<0 ){
               System.err.println( "No sent for receive event " + e );
               return;
           }
           Slot s = slots.get(slotno);
           placeBar(e.time, slotno, s);
           slots.set(slotno, null );
           cleanSlots();
       }
    }

    /**
     * @param endTime
     * @param slotno
     * @param s
     */
    private static void placeBar(long endTime, int slotno, Slot s) {
        printSVGBar( timeScale*(s.start-startTime), slotOffset+(slotno*slotSeparation), timeScale*(endTime-s.start) );
           printText( timeScale*(s.start-startTime), slotOffset+textSeparation+(slotno*slotSeparation), s.label );
    }

    private static void printSVGEvents()
    {
        System.out.println( "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>" );
        System.out.println( "<svg>" );
        while( !events.isEmpty() ) {
            TraceEvent e = events.poll();
            printSVGEvent( e );
        }
        while( slots.size()>0 ){
            int ix = slots.size()-1;
            Slot s = slots.get(ix);

            if( s != null ){
                placeBar(endTime, ix, s);
                System.err.println( "Outstanding slot" + s );
            }
            slots.remove( ix );

        }
        System.out.println( "</svg>" );
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
	for( String fnm: args ) {
	    readFile( fnm );
	}
        if( false ){
	printEvents();
        }
        else {
            printSVGEvents();
        }
    }

}
