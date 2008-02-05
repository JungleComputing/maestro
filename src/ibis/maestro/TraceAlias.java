package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * An alias declaration event.
 * 
 * This trace event announces that the given receive ports belong to the
 * same node.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class TraceAlias extends TraceEvent {
    final ReceivePortIdentifier porta;
    final ReceivePortIdentifier portb;

    /** */
    private static final long serialVersionUID = -7235865547532456639L;

    TraceAlias( ReceivePortIdentifier porta, ReceivePortIdentifier portb )
    {
	super(System.nanoTime() );
	this.porta = porta;
	this.portb = portb;
    }

}
