package ibis.maestro;

import java.io.Serializable;

/**
 * Abstract superclass of messages that the master sends to the worker.
 * @author Kees van Reeuwijk
 *
 */
public abstract class MasterMessage implements Serializable {

    /** Contractual obligation. */
    private static final long serialVersionUID = 1547379144090317151L;
}
