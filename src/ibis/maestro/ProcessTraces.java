package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

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
    private static double slotOffset = 40;
    private static double slotSeparation = 100;
    private static double textSeparation = 20;
    private static HashMap<ReceivePortIdentifier, Integer> portMap = new HashMap<ReceivePortIdentifier,Integer>();
    private static final ArrayList<Slot> slots = new ArrayList<Slot>();

    private static final double timeScale = 0.000003;

    private static class Slot {
        final long id;
        final long start;
        final String label;

        /**
         * Constructs a new slot in the trace output of transmission events.
         * @param id The job id associated with the transmission event.
         * @param start The start time in ms of the transmission event.
         * @param label The label of the transmission event.
         */
        public Slot(final long id, final long start, final String label)
        {
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
            System.out.println( "registerPort: P" + portMap.get(p) + ": " + p );
        }
    }

    private static void registerEvent( TraceEvent ev )
    {
        long t = ev.time;

        if( t<startTime ){
            startTime = t;
        }
        if( t>endTime ){
            endTime = t;
        }
        if( ev instanceof TransmissionEvent ) {
            TransmissionEvent e = (TransmissionEvent) ev;
            registerPort( e.source );
            registerPort( e.dest );
        }
        else if( ev instanceof TraceAlias ) {
            TraceAlias a = (TraceAlias) ev;
            registerAlias( a.porta, a.portb );
        }
        else if( ev instanceof WorkerRegistrationEvent ) {
            WorkerRegistrationEvent e = (WorkerRegistrationEvent) ev;
            
            registerPort( e.master );
            registerPort( e.worker );
        }
        else if( ev instanceof WorkerSettingEvent ) {
            WorkerSettingEvent e = (WorkerSettingEvent) ev;
            
            registerPort( e.master );
            registerPort( e.worker );
        }
        else {
            System.err.println( "Don't know how to register an event of type " + ev.getClass() );
        }
    }

    private static void readFile( String fnm )
    {
	try {
	    InputStream fis = new FileInputStream( fnm );
	    ObjectInputStream in = new ObjectInputStream( fis );
	    while( true ) {
		TraceEvent res = (TraceEvent) in.readObject();
		if( res == null ) {
                    // End of the trace file is marked by a null entry.
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
    
    private static void printSVGBar( double x, double y, double length )
    {
        System.out.println( "<g><path" );
        System.out.println( "  style=\"stroke:#000000;stroke-width:1px\"" );
        System.out.println( "  d=\"M" + x + ',' + y + " H" + (x+length) + "\"" );
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

    private static String spaces( int n ) {
	StringBuffer buf = new StringBuffer( n );
	
	for( int i=0; i<n; i++ ) {
	    buf.append( ' ' );
	}
	return new String( buf );
    }

    private static void registerAlias( ReceivePortIdentifier source, ReceivePortIdentifier dest )
    {
        if( portMap.containsKey( source ) ){
            portMap.put(dest, portMap.get( source ) );
        }
        else if( portMap.containsKey( dest ) ){
            portMap.put(source, portMap.get( dest ) );
        }
        else {
            int n = portNo++;
            portMap.put( source, n );
            portMap.put( dest, n );
            System.out.println( "registerAlias: P" + n + ": " + source );
            System.out.println( "registerAlias: P" + n + ": " + dest );
        }
    }

    private static String getHostName( ReceivePortIdentifier p )
    {
	if( portMap.containsKey(p) ) {
	    return "P" + portMap.get( p );
	}
	return p.toString();
    }

    private static void printEvent( TraceEvent ev )
    {
	long timeFromStart = ev.time-startTime;

	if( ev instanceof TraceAlias ){
            TraceAlias a = (TraceAlias) ev;
            
            System.out.println( "@" + Service.formatNanoseconds(timeFromStart) + " alias " + a.porta + "," + a.portb );
	}
	else if( ev instanceof WorkerRegistrationEvent ) {
	    WorkerRegistrationEvent e = (WorkerRegistrationEvent) ev;
	    
	    String master = getHostName( e.master );
	    String worker = getHostName( e.worker );
	    System.out.println( "@" + Service.formatNanoseconds(timeFromStart) + " master " + master + " registered worker " + worker );
	}
	else if( ev instanceof WorkerSettingEvent ) {
	    WorkerSettingEvent e = (WorkerSettingEvent) ev;
	    
	    String master = getHostName( e.master );
	    String worker = getHostName( e.worker );
            String info = e.getInfo();
	    System.out.println( "@" + Service.formatNanoseconds(timeFromStart) + " master " + master + " set worker " + worker + " " + info  );
	}
	else if( ev instanceof TransmissionEvent ) {
	    TransmissionEvent e = (TransmissionEvent) ev;
	    if( e.sent ){
		String lbl = e.getDescription( portMap );
		Slot s = new Slot( e.id, e.time, lbl );
		int slotno = slots.size();
		slots.add( s );
		System.out.println( spaces( slotno ) + '@' + Service.formatNanoseconds(timeFromStart) + " sent " + lbl );
	    }
	    else {
		// A received event.
		int slotno = searchSlot( e.id );
		if( slotno<0 ){
		    System.out.println( "@"+Service.formatNanoseconds(timeFromStart) + " No sent for receive event " + e );
		    return;
		}
		Slot s = slots.get(slotno);
		System.out.println( spaces( slotno ) + '@' + Service.formatNanoseconds(timeFromStart) + " recv " + s.label + " (" + Service.formatNanoseconds(e.time-s.start) + ')' );
		slots.set(slotno, null );
		cleanSlots();
	    }
	}
	else {
	    System.err.println( "Don't know how to print a trace event of type " + ev.getClass() );
	}
    }

    private static void printEvents()
    {
	while( !events.isEmpty() ) {
	    TraceEvent e = events.poll();
	    printEvent( e );
        }
        while( slots.size()>0 ){
            int ix = slots.size()-1;
            Slot s = slots.get(ix);

            if( s != null ){
        	System.out.println( spaces( ix ) + "no receive for message " + s.label );
            }
            slots.remove( ix );
        }
    }

    /** Returns the index in the list of slots of the message with
     * the given id, or -1 if there is no such slot.
     * @param id The identifier of the slot.
     * @return The index in the list of slots, or -1 if not found.
     */
    private static int searchSlot( long id )
    {
        for( int i=0; i<slots.size(); i++ ){
            Slot s = slots.get( i );

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
            Slot s = slots.get( ix );
            if( s != null ){
                break;
            }
            slots.remove( ix );
        }
    }
    
    /** Returns the index of a slot for the event. */
    private static int allocateSlot(){
        for( int i=0; i<slots.size(); i++ ){
            if( slots.get(i) == null ){
                return i;
            }
        }
        slots.add( null );
        return slots.size()-1;
    }

    private static void printSVGEvent( TraceEvent ev )
    {
	if( ev instanceof TraceAlias ){
	    TraceAlias ta = (TraceAlias) ev;
	    registerAlias( ta.porta, ta.portb );
	    return;
	}
	else if( ev instanceof TransmissionEvent ) {
	    TransmissionEvent e = (TransmissionEvent) ev;
	    if( e.sent ){
		String lbl = e.getDescription( portMap );
		int slotno = allocateSlot();
		Slot s = new Slot( e.id, e.time, lbl );
		slots.set( slotno, s );
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
	else {
	    System.err.println( "Don't know how to print a trace event of type " + ev.getClass() );
	}
    }

    /**
     * @param endTime
     * @param slotno
     * @param s
     */
    private static void placeBar( long endTime, int slotno, Slot s)
    {
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
            Slot s = slots.get( ix );

            if( s != null ){
                placeBar( endTime, ix, s );
                System.err.println( "Outstanding slot" + s.label );
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
        if( true ){
            printEvents();
        }
        else {
            printSVGEvents();
        }
    }

}
