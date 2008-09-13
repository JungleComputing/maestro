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

    int queueLength;
    int queueLengthSequenceNumber;
    long dequeueTimePerTask;
    long computeTime;

    /**
     * @param queueLength The worker queue length.
     */
    WorkerQueueInfo( int queueLength, int queueLengthSequenceNumber, long dequeueTimePerTask, long computeTime )
    {
	this.queueLength = queueLength;
	this.queueLengthSequenceNumber = queueLengthSequenceNumber;
	this.dequeueTimePerTask = dequeueTimePerTask;
	this.computeTime = computeTime;
    }

    /**
     * Returns a string representation of this completion info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "(ql=" + queueLength + ",dq/t=" + Utils.formatNanoseconds( dequeueTimePerTask ) + ",compute=" + Utils.formatNanoseconds( computeTime ) + ")";
    }

    String format()
    {
        return String.format( "%3d %9s %9s", queueLength, Utils.formatNanoseconds( dequeueTimePerTask ), Utils.formatNanoseconds( computeTime ) );
    }

    static String topLabel()
    {
        return String.format( "%3s %9s %9s", "ql", "dequeue", "compute" );
    }
    
    static String emptyFormat()
    {
        return String.format( "%23s", "---" );
    }

    static Object topLabelType( TaskType type )
    {
        return String.format( "%23s", type.toString() );
    }

    void failTask()
    {
	computeTime = Long.MAX_VALUE;
    }

    void setComputeTime( long t )
    {
	computeTime = t;
    }

    synchronized void setQueueTimePerTask( long queueInterval )
    {
	this.dequeueTimePerTask = queueInterval/this.queueLength;
        this.queueLength--;
	this.queueLengthSequenceNumber++;
    }

    synchronized void incrementQueueLength()
    {
	this.queueLength++;
	this.queueLengthSequenceNumber++;
    }
}
