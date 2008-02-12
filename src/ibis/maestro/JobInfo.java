package ibis.maestro;

/**
 * Information about a particular type of job. Contrary to WorkerJobInfo,
 * this information is independent of the particular worker.
 * @author Kees van Reeuwijk
 *
 */
public class JobInfo {

    /** Average size of job submission messages, or -1 if unknown. */
    private long sendSize= -1;
    
    /** Average size of job result messages, or -1 if unknown. */
    private long receiveSize = -1;
    
    /**
     * Update the estimate for the job message size with a new actual
     * message size.
     * @param sz The size of a message.
     */
    public void updateSendSize( long sz ) {
	if( sendSize<0 ) {
	    sendSize = sz;
	}
	else {
	    if( sz>0 ) {
		sendSize = (sendSize+sz)/2;
	    }
	}
    }

    /**
     * Update the estimated result message size with a new message size
     * for the result.
     * @param sz The new message size of a result message.
     */
    public void updateReceiveSize( long sz ) {
	if( receiveSize<0 ) {
	    receiveSize = sz;
	}
	else {
	    if( sz>0 ) {
		receiveSize = (receiveSize+sz)/2;
	    }
	}
    }

    public long getSendSize() {
	return sendSize;
    }
    
    public long getReceiveSize()
    {
	return receiveSize;
    }
}
