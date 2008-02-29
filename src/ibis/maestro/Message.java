package ibis.maestro;

import java.io.Serializable;

/**
 * A message in the Maestro system.
 * 
 * All message contain an id and a source port that together uniquely
 * identify them.
 * 
 * @author Kees van Reeuwijk
 *
 */
public abstract class Message implements Serializable {

    /** Contractual obligation. */
    private static final long serialVersionUID = 1547379144090317151L;
    
    /** The source of this message. */
    public final int source;

    /**
     * Constructs a new message from the given source.
     * We also generate a message id on the spot.
     * @param source The source of this message.
     */
    public Message( int source ) {
	this.source = source;
    }
}