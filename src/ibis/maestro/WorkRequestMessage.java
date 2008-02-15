package ibis.maestro;

import java.util.ArrayList;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message telling a master that we would like to receive jobs.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class WorkRequestMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    final ArrayList<JobType> allowedTypes;

    /**
     * Constructs a new work request message.
     * @param worker Who is asking for work?
     * @param allowedTypes Which types of jobs can it handle?
     */
    WorkRequestMessage( ReceivePortIdentifier worker, ArrayList<JobType> allowedTypes ){
	super( worker );
	this.allowedTypes = allowedTypes;
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TransmissionEvent.Type getMessageType()
    {
	return TransmissionEvent.Type.ASK_WORK;
    }
}
