package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

public class TraceAlias extends TraceEvent {
    final ReceivePortIdentifier source;
    final ReceivePortIdentifier dest;

    /** */
    private static final long serialVersionUID = -7235865547532456639L;

    TraceAlias( ReceivePortIdentifier source, ReceivePortIdentifier dest )
    {
	super(System.nanoTime() );
	this.source = source;
	this.dest = dest;
    }

}
