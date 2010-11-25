package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;

/**
 * A message in the Maestro system.
 * 
 * All message contain an id and a source port that together uniquely identify
 * them.
 * 
 * @author Kees van Reeuwijk
 * 
 */
abstract class Message implements Serializable {

    /** Contractual obligation. */
    private static final long serialVersionUID = 1547379144090317151L;

    transient double arrivalMoment;

    transient IbisIdentifier source;
}
