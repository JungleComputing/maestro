package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A simple class to store a task, worker pair.
 * 
 * @author Kees van Reeuwijk
 *
 */
class Submission {
    TaskInstance task = null;
    IbisIdentifier worker = null;
    long predictedDuration = 0L;
}
