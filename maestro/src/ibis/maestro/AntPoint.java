/**
 * 
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;

/**
 * One point in an ant trail.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class AntPoint implements Serializable {
    private static final long serialVersionUID = 1L;

    final IbisIdentifier masterIbis;

    final IbisIdentifier workerIbis;

    final double timestamp;

    final int typeIndex;

    /**
     * @param masterIbis
     *            The master of this task submission.
     * @param workerIbis
     *            The worker of this task submission.
     * @param timestamp
     *            The timestamp of the task submission.
     * @param typeIndex
     *            The type of task.
     */
    AntPoint(final IbisIdentifier masterIbis, final IbisIdentifier workerIbis,
            final double timestamp, final int typeIndex) {
        this.masterIbis = masterIbis;
        this.workerIbis = workerIbis;
        this.timestamp = timestamp;
        this.typeIndex = typeIndex;
    }

    /**
     * Returns a string representation of this ant routing.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "ant routing for type " + Globals.allTaskTypes[typeIndex] + " @"
                + timestamp + ": " + masterIbis + "->" + workerIbis;
    }
}
