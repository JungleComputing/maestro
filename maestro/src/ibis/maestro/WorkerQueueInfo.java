package ibis.maestro;

import java.io.Serializable;

/**
 * A class representing the current worker queue length for the given type.
 *
 * @author Kees van Reeuwijk
 */
class WorkerQueueInfo implements Serializable
{
    private static final long serialVersionUID = 1L;

    final int queueLength;
    final long dequeueTime;
    final long computeTime;

    /**
     * @param queueLength The worker queue length.
     */
    WorkerQueueInfo( int queueLength, long dequeueTime, long computeTime )
    {
	this.queueLength = queueLength;
	this.dequeueTime = dequeueTime;
	this.computeTime = computeTime;
    }

    /**
     * Returns a string representation of this completion info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "WorkerQueueInfo: queueLength=" + queueLength + " dequeueTime=" + Service.formatNanoseconds( dequeueTime ) + " computeTime=" + Service.formatNanoseconds( computeTime );
    }

    String format()
    {
        return String.format( "%3d %9s %9s", queueLength, Service.formatNanoseconds( dequeueTime ), Service.formatNanoseconds( computeTime ) );
    }
}
